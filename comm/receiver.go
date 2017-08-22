package comm

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"crypto/tls"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// Structs

// Receiver bundles all information needed to accept
// and process incoming CRDT downstream messages.
type Receiver struct {
	lock             *sync.Mutex
	logger           log.Logger
	name             string
	listenAddr       string
	publicAddr       string
	msgInLog         chan struct{}
	socket           net.Listener
	tlsConfig        *tls.Config
	writeLog         *os.File
	updLog           *os.File
	incVClock        chan string
	updVClock        chan map[string]uint32
	vclock           map[string]uint32
	vclockLog        *os.File
	stopTrigger      chan struct{}
	stopApply        chan struct{}
	applyCRDTUpdChan chan Msg
	doneCRDTUpdChan  chan struct{}
	nodes            map[string]string
}

// Functions

// InitReceiver initializes above struct and sets
// default values. It starts involved background
// routines and send initial channel trigger.
func InitReceiver(logger log.Logger, name string, listenAddr string, publicAddr string, logFilePath string, vclockLogPath string, socket net.Listener, tlsConfig *tls.Config, applyCRDTUpdChan chan Msg, doneCRDTUpdChan chan struct{}, nodes map[string]string) (chan string, chan map[string]uint32, error) {

	// Create and initialize new struct.
	recv := &Receiver{
		lock:             &sync.Mutex{},
		logger:           logger,
		name:             name,
		listenAddr:       listenAddr,
		publicAddr:       publicAddr,
		msgInLog:         make(chan struct{}, 1),
		socket:           socket,
		tlsConfig:        tlsConfig,
		incVClock:        make(chan string),
		updVClock:        make(chan map[string]uint32),
		vclock:           make(map[string]uint32),
		stopTrigger:      make(chan struct{}),
		stopApply:        make(chan struct{}),
		applyCRDTUpdChan: applyCRDTUpdChan,
		doneCRDTUpdChan:  doneCRDTUpdChan,
		nodes:            nodes,
	}

	// Open log file descriptor for writing.
	write, err := os.OpenFile(logFilePath, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("opening CRDT log file for writing failed with: %v", err)
	}
	recv.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("opening CRDT log file for updating failed with: %v", err)
	}
	recv.updLog = upd

	// Initially, reset position in update file to beginning.
	_, err = recv.updLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, nil, fmt.Errorf("could not reset position in update CRDT log file: %v", err)
	}

	// Initially set vector clock entries to 0.
	for node := range nodes {
		recv.vclock[node] = 0
	}

	// Including the entry of this node.
	recv.vclock[name] = 0

	// Open log file of last known vector clock values.
	vclockLog, err := os.OpenFile(vclockLogPath, (os.O_CREATE | os.O_RDWR), 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("opening vector clock log failed with: %v", err)
	}
	recv.vclockLog = vclockLog

	// Initially, reset position in vector clock file to beginning.
	_, err = recv.vclockLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, nil, fmt.Errorf("could not reset position in vector clock log: %v", err)
	}

	// If vector clock entries were preserved, set them.
	err = recv.SetVClockEntries()
	if err != nil {
		return nil, nil, fmt.Errorf("reading in stored vector clock entries failed: %v", err)
	}

	// Start routine in background that takes care of
	// vector clock increments.
	go recv.IncVClockEntry()

	// Initialize and run a new gRPC server with appropriate
	// options set to send and receive CRDT updates.
	go recv.StartGRPCRecv()

	// Apply received messages in background.
	go recv.ApplyStoredMsgs()

	// If we just started the application, perform an
	// initial run to check if log file contains elements.
	recv.msgInLog <- struct{}{}

	// Start triggering msgInLog events periodically.
	go recv.TriggerMsgApplier(5)

	return recv.incVClock, recv.updVClock, nil
}

// StartGRPCRecv initializes and runs a configured
// gRPC receiver for pluto-internal communication.
func (recv *Receiver) StartGRPCRecv() error {

	// Define options for an empty gRPC server.
	options := ReceiverOptions(recv.tlsConfig)
	grpcRecv := grpc.NewServer(options...)

	// Register the empty server on fulfilling interface.
	RegisterReceiverServer(grpcRecv, recv)

	level.Info(recv.logger).Log(
		"msg", "accepting CRDT sync connections",
		"public_addr", recv.publicAddr,
		"listen_addr", recv.listenAddr,
	)

	// Run server.
	return grpcRecv.Serve(recv.socket)
}

// TriggerMsgApplier starts a timer that triggers
// an msgInLog event when duration elapsed. Supposed
// to routinely poke the ApplyStoredMsgs into checking
// for unprocessed messages in log.
func (recv *Receiver) TriggerMsgApplier(waitSeconds time.Duration) {

	// Specify duration to wait between triggers.
	triggerD := waitSeconds * time.Second

	// Create a timer that waits for one second
	// to elapse and then fires.
	triggerT := time.NewTimer(triggerD)

	for {

		select {

		case <-recv.stopTrigger:

			// If stop channel was activated,
			// shut down trigger and return.
			triggerT.Stop()
			return

		case _, ok := <-triggerT.C:
			if ok {

				// If buffered channel indicating an arrived
				// msg is not full yet, make it full.
				if len(recv.msgInLog) < 1 {
					recv.msgInLog <- struct{}{}
				}

				// Renew timer.
				triggerT.Reset(triggerD)
			}
		}
	}
}

// Incoming is the main handler for CRDT downstream synchronization
// messages reaching a receiver. It accepts transported binary messages
// and writes their content to the designted receiving log file. Finally,
// a trigger is sent to the application routine.
func (recv *Receiver) Incoming(stream Receiver_IncomingServer) error {

	// Accept a *BinMsg from stream.
	binMsg, err := stream.Recv()
	if err != nil {

		level.Error(recv.logger).Log(
			"msg", "error during Recv() at receiver",
			"err", err,
		)

		return err
	}

	// Prepend binary message with length in bytes.
	// TODO: Make this fast?
	data := append([]byte(fmt.Sprintf("%d;", len(binMsg.Data))), binMsg.Data...)

	// Lock mutex.
	recv.lock.Lock()

	// Write it to message log file.
	_, err = recv.writeLog.Write(data)
	if err != nil {
		return err
	}

	// Save to stable storage.
	err = recv.writeLog.Sync()
	if err != nil {
		return err
	}

	// Unlock mutex.
	recv.lock.Unlock()

	// Indicate to applying routine that a new message
	// is available to process.
	if len(recv.msgInLog) < 1 {
		recv.msgInLog <- struct{}{}
	}

	// Confirm message to sending node.
	err = stream.Send(&Conf{
		Status: 0,
	})
	if err != nil {
		return err
	}

	return nil
}

// ApplyStoredMsgs waits for a signal on a channel that
// indicates a new available message to process, reads and
// updates the CRDT log file and applies the payload to
// the CRDT state.
func (recv *Receiver) ApplyStoredMsgs() {

	for {

		select {

		case <-recv.stopApply:
			return

		// Wait for signal that new message was written to
		// log file so that we can process it.
		case _, ok := <-recv.msgInLog:
			if ok {

				// Lock mutex.
				recv.lock.Lock()

				// Most of the following commands are taking from
				// this stackoverflow answer describing a way to
				// pop the first line of a file and write back
				// the remaining parts:
				// http://stackoverflow.com/a/30948278
				info, err := recv.updLog.Stat()
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "could not get CRDT log file information",
						"err", err,
					)
					os.Exit(1)
				}

				// Store accessed file size for multiple use.
				logSize := info.Size()

				// Check if log file is empty and continue at next
				// for loop iteration if that is the case.
				if logSize == 0 {
					recv.lock.Unlock()
					continue
				}

				// Save current position of head for later use.
				curOffset, err := recv.updLog.Seek(0, os.SEEK_CUR)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "error while retrieving current head position in CRDT log file",
						"err", err,
					)
					os.Exit(1)
				}

				// Calculate size of needed buffer.
				bufferSize := logSize - curOffset

				// Account for case when offset reached end of log file
				// or accidentally the current offset is bigger than the
				// log file's size.
				if logSize <= curOffset {

					// Reset position to beginning of file.
					_, err = recv.updLog.Seek(0, os.SEEK_SET)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "could not reset position in CRDT log file",
							"err", err,
						)
						os.Exit(1)
					}

					// Unlock log file mutex.
					recv.lock.Unlock()

					// Send signal again to check for next log items.
					if len(recv.msgInLog) < 1 {
						recv.msgInLog <- struct{}{}
					}

					// Go to next loop iteration.
					continue
				}

				// Create a buffer of capacity of read file size
				// minus the current head position offset.
				buf := bytes.NewBuffer(make([]byte, 0, bufferSize))

				// Copy contents of log file to prepared buffer.
				_, err = io.Copy(buf, recv.updLog)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "could not copy CRDT log file contents to buffer",
						"err", err,
					)
					os.Exit(1)
				}

				// Read byte amount of following binary message.
				numBytesRaw, err := buf.ReadBytes(';')
				if (err != nil) && (err != io.EOF) {
					level.Error(recv.logger).Log(
						"msg", "error extracting number of bytes of first message in CRDT log file",
						"err", err,
					)
					os.Exit(1)
				}

				// Convert string to number.
				numBytes, err := strconv.ParseInt((string(numBytesRaw[:(len(numBytesRaw) - 1)])), 10, 64)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "failed to convert string to int indicating number of bytes",
						"err", err,
					)
					os.Exit(1)
				}

				// Reserve exactly enough space for current message.
				msgRaw := make([]byte, numBytes)

				// Read current message from buffer of log file.
				numRead, err := buf.Read(msgRaw)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "error during extraction of current message in CRDT log file",
						"err", err,
					)
					os.Exit(1)
				}

				if int64(numRead) != numBytes {
					level.Error(recv.logger).Log("msg", fmt.Sprintf("expected message of length %d, but only read %d", numBytes, numRead))
					os.Exit(1)
				}

				// Calculate total space of stored message:
				// number of bytes prefix + ';' + actual message.
				msgSize := int64(len(numBytesRaw)) + numBytes

				// Unmarshal read ProtoBuf into defined Msg struct.
				msg := &Msg{}
				err = proto.Unmarshal(msgRaw, msg)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "failed to unmarshal read ProtoBuf into defined Msg struct",
						"err", err,
					)
					os.Exit(1)
				}

				// Initially, set apply indicator to true. This means,
				// that the message would be considered for further parsing.
				applyMsg := true

				// Check if this message is an already received or
				// the expected next one from the sending node.
				// If not, set indicator to false.
				if (msg.Vclock[msg.Replica] != recv.vclock[msg.Replica]) &&
					(msg.Vclock[msg.Replica] != (recv.vclock[msg.Replica] + 1)) {
					applyMsg = false
				}

				for node, value := range msg.Vclock {

					if node != msg.Replica {

						// Next, range over all received vector clock values
						// and check if they do not exceed the locally stored
						// values for these nodes.
						if value > recv.vclock[node] {
							applyMsg = false
							break
						}
					}
				}

				// If this indicator is false, there are messages not yet
				// processed at this node that causally precede the just
				// parsed message. We therefore cycle to the next message.
				if applyMsg {

					// If this message is actually the next expected one,
					// process its contents with CRDT logic. This ensures
					// that message duplicates will get purged but not applied.
					if msg.Vclock[msg.Replica] == (recv.vclock[msg.Replica] + 1) {

						// Pass payload for higher-level interpretation
						// to channel connected to node.
						recv.applyCRDTUpdChan <- *msg

						// Wait for done signal from node.
						<-recv.doneCRDTUpdChan
					}

					for node, value := range msg.Vclock {

						// Adjust local vector clock to continue with pair-wise
						// maximum of the vector clock elements.
						if value > recv.vclock[node] {
							recv.vclock[node] = value
						}
					}

					// Save updated vector clock to log file.
					err := recv.SaveVClockEntries()
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "saving updated vector clock to file failed",
							"err", err,
						)
						os.Exit(1)
					}

					// Reset head position to curOffset saved at beginning of loop.
					_, err = recv.updLog.Seek(curOffset, os.SEEK_SET)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "failed to reset updLog head to saved position",
							"err", err,
						)
						os.Exit(1)
					}

					// Copy reduced buffer contents back to current position
					// of CRDT log file, effectively deleting the read message.
					newNumOfBytes, err := io.Copy(recv.updLog, buf)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "error during copying buffer contents back to CRDT log file",
							"err", err,
						)
						os.Exit(1)
					}

					// Now, truncate log file size to (curOffset + newNumOfBytes),
					// reducing the file size by length of handled message.
					err = recv.updLog.Truncate((curOffset + newNumOfBytes))
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "could not truncate CRDT log file",
							"err", err,
						)
						os.Exit(1)
					}

					// Sync changes to stable storage.
					err = recv.updLog.Sync()
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "syncing CRDT log file to stable storage failed with",
							"err", err,
						)
						os.Exit(1)
					}

					// Reset position to beginning of file because the
					// chances are high that we now can proceed in order
					// of CRDT message log file.
					_, err = recv.updLog.Seek(0, os.SEEK_SET)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "could not reset position in CRDT log file",
							"err", err,
						)
						os.Exit(1)
					}
				} else {

					level.Warn(recv.logger).Log("msg", "message was out of order, taking next one")

					// Set position of head to byte after just read message,
					// effectively delaying execution of that message.
					_, err = recv.updLog.Seek((curOffset + msgSize), os.SEEK_SET)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "error while moving position in CRDT log file to next line",
							"err", err,
						)
						os.Exit(1)
					}
				}

				// Unlock mutex.
				recv.lock.Unlock()

				// We do not know how many elements are waiting in the
				// log file. Therefore attempt to process next one and
				// if it does not exist, the loop iteration will abort.
				if len(recv.msgInLog) < 1 {
					recv.msgInLog <- struct{}{}
				}
			}
		}
	}
}
