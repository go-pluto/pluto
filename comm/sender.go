package comm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"crypto/tls"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
func InitSender(logger log.Logger, name string, logFilePath string, tlsConfig *tls.Config, incVClock chan string, updVClock chan map[string]uint32, downSender chan struct{}, nodes map[string]string) (chan Msg, error) {

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
		return nil, fmt.Errorf("[comm.InitSender] Opening CRDT log file for writing failed with: %v", err)
	}
	sender.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("[comm.InitSender] Opening CRDT log file for updating failed with: %v", err)
	}
	sender.updLog = upd

	// Prepare gRPC call options for later use.
	sender.gRPCOptions = SenderOptions(sender.tlsConfig)

	if name == "worker-1" {

		if err := sender.TestGRPC(&Msg{
			Operation: "ROFL",
		}); err != nil {
			sender.logger.Log("msg", fmt.Sprintf("gRPC test failed: '%#v'", err))
			os.Exit(1)
		}
	}

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

func (sender *Sender) TestGRPC(msg *Msg) error {

	conn, err := grpc.Dial(sender.nodes["storage"], sender.gRPCOptions...)
	if err != nil {
		return errors.Wrap(err, "[TEST 1]")
	}
	defer conn.Close()

	client := NewReceiverClient(conn)

	closed, err := client.Incoming(context.Background(), msg)
	if err != nil {
		return errors.Wrap(err, "[TEST 2]")
	}

	sender.logger.Log("msg", fmt.Sprintf("connection to server closed with: '%#v'", closed))

	return nil
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
				level.Error(sender.logger).Log("msg", fmt.Sprintf("failed to marshal enriched downstream Msg to ProtoBuf: %v", err))
				os.Exit(1)
			}
			data = append(data, '\n')

			sender.logger.Log("msg", fmt.Sprintf("[TODO] all bytes of marshalled msg: '%#v' (last: '%#v')\n\tString representation: '%s'", data, data[:(len(data)-1)], data))

			// Write it to message log file.
			_, err = sender.writeLog.Write(data)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("writing to CRDT log file failed with: %v", err))
				os.Exit(1)
			}

			// Save to stable storage.
			err = sender.writeLog.Sync()
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("syncing CRDT log file to stable storage failed with: %v", err))
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
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not get CRDT log file information: %v", err))
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
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not reset position in CRDT log file: %v", err))
				os.Exit(1)
			}

			// Copy contents of log file to prepared buffer.
			_, err = io.Copy(buf, sender.updLog)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not copy CRDT log file contents to buffer: %v", err))
				os.Exit(1)
			}

			// Read oldest message from log file.
			payload, err := buf.ReadBytes('\n')
			if (err != nil) && (err != io.EOF) {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("error during extraction of first line in CRDT log file: %v", err))
				os.Exit(1)
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not reset position in CRDT log file: %v", err))
				os.Exit(1)
			}

			// Unlock mutex.
			sender.lock.Unlock()

			sender.logger.Log("msg", fmt.Sprintf("[TODO] BEFORE newline remove: '%#v'", payload))
			// Remove trailing newline symbol from payload.
			payload = payload[:(len(payload) - 1)]
			sender.logger.Log("msg", fmt.Sprintf("[TODO] AFTER newline remove: '%#v'", payload))

			// Unmarshal stored ProtoBuf Msg into Msg struct.
			msg := &Msg{}
			if err := proto.Unmarshal(payload, msg); err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("failed to unmarshal stored ProtoBuf Msg into Msg struct: %v", err))
				os.Exit(1)
			}

			// TODO: Parallelize this loop?
			for node, addr := range sender.nodes {

				// Connect to downstream replica.
				conn, err := grpc.Dial(addr, sender.gRPCOptions...)
				if err != nil {
					level.Error(sender.logger).Log("msg", fmt.Sprintf("could not connect to downstream replica %s: %v", node, err))
					os.Exit(1)
				}
				defer conn.Close()

				// Create new gRPC client stub.
				client := NewReceiverClient(conn)

				// Send msg to downstream replica.
				_, err = client.Incoming(context.Background(), msg)
				if err != nil {
					level.Error(sender.logger).Log("msg", fmt.Sprintf("could not send downstream message to replica %s: %v", node, err))
					os.Exit(1)
				}
			}

			// Lock mutex.
			sender.lock.Lock()

			// Retrieve file information.
			info, err = sender.updLog.Stat()
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not get CRDT log file information: %v", err))
				os.Exit(1)
			}

			// Create a buffer of capacity of read file size.
			buf = bytes.NewBuffer(make([]byte, 0, info.Size()))

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not reset position in CRDT log file: %v", err))
				os.Exit(1)
			}

			// Copy contents of log file to prepared buffer.
			_, err = io.Copy(buf, sender.updLog)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not copy CRDT log file contents to buffer: %v", err))
				os.Exit(1)
			}

			// Read oldest message from log file.
			_, err = buf.ReadString('\n')
			if (err != nil) && (err != io.EOF) {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("error during extraction of first line in CRDT log file: %v", err))
				os.Exit(1)
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not reset position in CRDT log file: %v", err))
				os.Exit(1)
			}

			// Copy reduced buffer contents back to beginning
			// of CRDT log file, effectively deleting the first line.
			newNumOfBytes, err := io.Copy(sender.updLog, buf)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("error during copying buffer contents back to CRDT log file: %v", err))
				os.Exit(1)
			}

			// Now, truncate log file size to exact amount
			// of bytes copied from buffer.
			err = sender.updLog.Truncate(newNumOfBytes)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not truncate CRDT log file: %v", err))
				os.Exit(1)
			}

			// Sync changes to stable storage.
			err = sender.updLog.Sync()
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("syncing CRDT log file to stable storage failed with: %v", err))
				os.Exit(1)
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				level.Error(sender.logger).Log("msg", fmt.Sprintf("could not reset position in CRDT log file: %v", err))
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
