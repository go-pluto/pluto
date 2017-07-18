package comm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"crypto/tls"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Structs

// Sender bundles information needed for sending
// out sync messages via CRDTs.
type Sender struct {
	lock        *sync.Mutex
	logger      log.Logger
	name        string
	tlsConfig   *tls.Config
	gRPCOptions []grpc.DialOption
	inc         chan Msg
	msgInLog    chan struct{}
	writeLog    *os.File
	updLog      *os.File
	incVClock   chan string
	updVClock   chan map[string]uint32
	nodes       map[string]string
}

// Functions

// InitSender initializes above struct and sets
// default values for most involved elements to start
// with. It returns a channel local processes can put
// CRDT changes into, so that those changes will be
// communicated to connected nodes.
func InitSender(logger log.Logger, name string, logFilePath string, tlsConfig *tls.Config, incVClock chan string, updVClock chan map[string]uint32, nodes map[string]string) (chan Msg, error) {

	// Create and initialize what we need for
	// a CRDT sender routine.
	sender := &Sender{
		lock:      &sync.Mutex{},
		logger:    logger,
		name:      name,
		tlsConfig: tlsConfig,
		inc:       make(chan Msg),
		msgInLog:  make(chan struct{}, 1),
		incVClock: incVClock,
		updVClock: updVClock,
		nodes:     nodes,
	}

	// Open log file descriptor for writing.
	write, err := os.OpenFile(logFilePath, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	if err != nil {
		return nil, fmt.Errorf("opening CRDT log file for writing failed with: %v", err)
	}
	sender.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening CRDT log file for updating failed with: %v", err)
	}
	sender.updLog = upd

	// Prepare gRPC call options for later use.
	sender.gRPCOptions = SenderOptions(sender.tlsConfig)

	// Start brokering routine in background.
	go sender.BrokerMsgs()

	// Start sending routine in background.
	go sender.SendMsgs()

	// If we just started the application, perform an
	// initial run to check if log file contains elements.
	sender.msgInLog <- struct{}{}

	// Return this channel to pass to processes.
	return sender.inc, nil
}

// BrokerMsgs awaits a CRDT message to send to downstream
// replicas from one of the local processes on channel inc.
// It stores the message for sending in a dedicated CRDT log
// file and passes on a signal that a new message is available.
func (sender *Sender) BrokerMsgs() {

	for {

		// Receive CRDT payload to send to other nodes
		// on incoming channel.
		payload, ok := <-sender.inc
		if ok {

			// Lock mutex.
			sender.lock.Lock()

			// Set this replica's name as sending part.
			payload.Replica = sender.name

			// Send this replica's name on incVClock channel to
			// request an increment of its vector clock value.
			sender.incVClock <- sender.name

			// Wait for updated vector clock to be sent back
			// on other defined channel.
			payload.Vclock = <-sender.updVClock

			// Marshal message according to ProtoBuf specification
			// and add a trailing newline symbol.
			data, err := proto.Marshal(&payload)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "failed to marshal enriched downstream Msg to ProtoBuf",
					"err", err,
				)
				os.Exit(1)
			}

			// Prepend binary message with length in bytes.
			// TODO: Make this fast?
			data = append([]byte(fmt.Sprintf("%d;", len(data))), data...)

			// Write it to message log file.
			_, err = sender.writeLog.Write(data)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "writing to CRDT log file failed with",
					"err", err,
				)
				os.Exit(1)
			}

			// Save to stable storage.
			err = sender.writeLog.Sync()
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "syncing CRDT log file to stable storage failed with",
					"err", err,
				)
				os.Exit(1)
			}

			// Unlock mutex.
			sender.lock.Unlock()

			// Inidicate consecutive loop iterations
			// that a message is waiting in log.
			if len(sender.msgInLog) < 1 {
				sender.msgInLog <- struct{}{}
			}
		}
	}
}

// SendMsgs waits for a signal indicating that a message
// is waiting in the log file to be send out and sends that
// to all downstream nodes.
func (sender *Sender) SendMsgs() {

	for {

		// Wait for signal that new message was written to
		// log file so that we can send it out.
		_, ok := <-sender.msgInLog
		if ok {

			// Lock mutex.
			sender.lock.Lock()

			// Most of the following commands are taking from
			// this stackoverflow answer describing a way to
			// pop the first line of a file and write back
			// the remaining parts:
			// http://stackoverflow.com/a/30948278
			info, err := sender.updLog.Stat()
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not get CRDT log file information",
					"err", err,
				)
				os.Exit(1)
			}

			// Check if log file is empty and continue at next
			// for loop iteration if that is the case.
			if info.Size() == 0 {
				sender.lock.Unlock()
				continue
			}

			// Create a buffer of capacity of read file size.
			buf := bytes.NewBuffer(make([]byte, 0, info.Size()))

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not reset position in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Copy contents of log file to prepared buffer.
			_, err = io.Copy(buf, sender.updLog)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not copy CRDT log file contents to buffer",
					"err", err,
				)
				os.Exit(1)
			}

			// Read byte amount of following binary message.
			numBytesRaw, err := buf.ReadBytes(';')
			if (err != nil) && (err != io.EOF) {
				level.Error(sender.logger).Log(
					"msg", "error extracting number of bytes of first message in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Reset position to byte directly after message length separator.
			_, err = sender.updLog.Seek(int64(len(numBytesRaw)), os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not change position in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Convert string to number.
			numBytes, err := strconv.ParseInt((string(numBytesRaw[:(len(numBytesRaw) - 1)])), 10, 64)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "failed to convert string to int indicating number of bytes",
					"err", err,
				)
				os.Exit(1)
			}

			// Reserve exactly enough space for current message
			// in downstream BinMsg.
			binMsg := &BinMsg{
				Data: make([]byte, numBytes),
			}

			// Read oldest message from log file.
			numRead, err := sender.updLog.Read(binMsg.Data)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "error during extraction of first message in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not reset position in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Unlock mutex.
			sender.lock.Unlock()

			// Check number of read bytes.
			if int64(numRead) != numBytes {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("expected message of length %d, but only read %d", numBytes, numRead))
				os.Exit(1)
			}

			// Calculate total space of stored message:
			// number of bytes prefix + ';' + actual message.
			msgSize := int64(len(numBytesRaw)) + numBytes

			wg := &sync.WaitGroup{}

			// Prepare buffered completion channel for routines.
			retStatus := make(chan string, len(sender.nodes))

			for node, addr := range sender.nodes {

				wg.Add(1)

				go func(retStatus chan string, node string, addr string, opts []grpc.DialOption, binMsg *BinMsg, logger log.Logger) {

					defer wg.Done()

					// Connect to downstream replica.
					conn, err := grpc.Dial(addr, opts...)
					for err != nil {
						level.Debug(logger).Log(
							"msg", "failed to dial, trying again...",
							"remote_node", node,
							"remote_addr", addr,
							"err", err,
						)
						conn, err = grpc.Dial(addr, opts...)
					}

					level.Debug(logger).Log(
						"msg", "successfully connected",
						"remote_node", node,
						"remote_addr", addr,
					)

					// Create new gRPC client stub.
					client := NewReceiverClient(conn)

					// Send BinMsg to downstream replica.
					conf, err := client.Incoming(context.Background(), binMsg)
					for err != nil {

						// If we received an error, examine it and
						// take appropriate action.
						stat, ok := status.FromError(err)
						if ok && (stat.Code() == codes.Unavailable) {
							level.Debug(logger).Log(
								"msg", "downstream replica unavailable during Incoming(), trying again...",
								"remote_node", node,
								"remote_addr", addr,
							)
							conf, err = client.Incoming(context.Background(), binMsg)
						} else {
							retStatus <- fmt.Sprintf("permanent error for sending downstream message to replica %s: %v", node, err)
							return
						}
					}

					if conf.Status != 0 {
						retStatus <- fmt.Sprintf("sending downstream message to %s returned code: %d", node, conf.Status)
					}

					// Indicate success via channel.
					retStatus <- "0"
				}(retStatus, node, addr, sender.gRPCOptions, binMsg, sender.logger)
			}

			for i := 0; i < len(sender.nodes); i++ {

				// Wait and check received string on channel.
				retStat := <-retStatus
				if retStat != "0" {
					level.Error(sender.logger).Log("msg", retStat)
					os.Exit(1)
				}
			}

			// Wait for all routines to having completed
			// the synchronization successfully.
			wg.Wait()

			// Lock mutex.
			sender.lock.Lock()

			// Retrieve file information.
			info, err = sender.updLog.Stat()
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not get CRDT log file information",
					"err", err,
				)
				os.Exit(1)
			}

			// Create a buffer of capacity of read file size.
			buf = bytes.NewBuffer(make([]byte, 0, (info.Size() - msgSize)))

			// Reset position to byte after first message in file.
			_, err = sender.updLog.Seek(msgSize, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not reset position in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Copy contents of log file to prepared buffer.
			_, err = io.Copy(buf, sender.updLog)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not copy CRDT log file contents to buffer",
					"err", err,
				)
				os.Exit(1)
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not reset position in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Copy reduced buffer contents back to beginning
			// of CRDT log file, effectively deleting the first line.
			newNumOfBytes, err := io.Copy(sender.updLog, buf)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "error during copying buffer contents back to CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Now, truncate log file size to exact amount
			// of bytes copied from buffer.
			err = sender.updLog.Truncate(newNumOfBytes)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not truncate CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Sync changes to stable storage.
			err = sender.updLog.Sync()
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "syncing CRDT log file to stable storage failed with",
					"err", err,
				)
				os.Exit(1)
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log(
					"msg", "could not reset position in CRDT log file",
					"err", err,
				)
				os.Exit(1)
			}

			// Unlock mutex.
			sender.lock.Unlock()

			// We do not know how many elements are waiting in the
			// log file. Therefore attempt to send next one and if
			// it does not exist, the loop iteration will abort.
			if len(sender.msgInLog) < 1 {
				sender.msgInLog <- struct{}{}
			}
		}
	}
}
