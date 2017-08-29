package comm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"crypto/tls"
	"io/ioutil"

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
	inc         chan Msg
	stopTrigger chan struct{}
	logFilePath string
	writeLog    *os.File
	updLog      *os.File
	incVClock   chan string
	updVClock   chan map[string]uint32
	nodes       map[string]string
	syncConns   map[string]ReceiverClient
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
		lock:        &sync.Mutex{},
		logger:      logger,
		name:        name,
		tlsConfig:   tlsConfig,
		inc:         make(chan Msg),
		stopTrigger: make(chan struct{}),
		logFilePath: logFilePath,
		incVClock:   incVClock,
		updVClock:   updVClock,
		nodes:       nodes,
		syncConns:   make(map[string]ReceiverClient),
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
	gRPCOptions := SenderOptions(sender.tlsConfig)

	for node, addr := range sender.nodes {

		// Connect to downstream replica.
		conn, err := grpc.Dial(addr, gRPCOptions...)
		for err != nil {
			level.Error(sender.logger).Log(
				"msg", "failed to dial, trying again...",
				"remote_node", node,
				"remote_addr", addr,
				"err", err,
			)
			conn, err = grpc.Dial(addr, gRPCOptions...)
		}

		level.Error(sender.logger).Log(
			"msg", "successfully connected",
			"remote_node", node,
			"remote_addr", addr,
		)

		// Create new gRPC client stub and save
		// it to synchronization map.
		sender.syncConns[node] = NewReceiverClient(conn)
	}

	// Start brokering routine in background.
	go sender.BrokerMsgs()

	// Start sending routine in background.
	go sender.SendMsgs(3)

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

			sender.lock.Unlock()
		}
	}
}

// SendMsgs waits for a signal indicating that a message
// is waiting in the log file to be send out and sends that
// to all downstream nodes.
func (sender *Sender) SendMsgs(waitSeconds time.Duration) {

	// Specify duration to wait between triggers.
	triggerD := waitSeconds * time.Second

	// Create a timer that waits for the specified
	// amount of seconds to elapse and then fires.
	triggerT := time.NewTimer(triggerD)

	for {

		select {

		case <-sender.stopTrigger:

			// If stop channel was activated,
			// shut down trigger and return.
			triggerT.Stop()
			return

		case _, ok := <-triggerT.C:
			if ok {

				sender.lock.Lock()

				// Retrieve file information.
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

					// Renew timer.
					triggerT.Reset(triggerD)

					continue
				}

				// Read current content of log file.
				data, err := ioutil.ReadFile(sender.logFilePath)
				if err != nil {
					level.Error(sender.logger).Log(
						"msg", "could not read content of CRDT log file",
						"err", err,
					)
					os.Exit(1)
				}

				// Wrap bytes buffer in message fit for
				// sending via gRPC function.
				binMsgs := &BinMsgs{
					Data: data,
				}

				sender.lock.Unlock()

				// Store size of all read msgs for later truncation.
				msgsSize := int64(len(data))

				wg := &sync.WaitGroup{}

				// Prepare buffered completion channel for routines.
				retStatus := make(chan string, len(sender.syncConns))

				for node, client := range sender.syncConns {

					wg.Add(1)

					go func(retStatus chan string, node string, addr string, client ReceiverClient, binMsgs *BinMsgs, logger log.Logger) {

						defer wg.Done()

						// Send BinMsgs to downstream replica.
						conf, err := client.Incoming(context.Background(), binMsgs)
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
								conf, err = client.Incoming(context.Background(), binMsgs)
							} else {
								retStatus <- fmt.Sprintf("permanent error for sending downstream messages to replica %s: %v", node, err)
								return
							}
						}

						if conf.Status != 0 {
							retStatus <- fmt.Sprintf("sending downstream messages to %s returned code: %d", node, conf.Status)
						}

						// Indicate success via channel.
						retStatus <- "0"
					}(retStatus, node, sender.nodes[node], client, binMsgs, sender.logger)
				}

				for i := 0; i < len(sender.syncConns); i++ {

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

				sender.lock.Lock()

				// Most of the following commands are taken from
				// this stackoverflow answer describing a way to
				// pop the first line of a file and write back
				// the remaining parts:
				// http://stackoverflow.com/a/30948278
				info, err = sender.updLog.Stat()
				if err != nil {
					level.Error(sender.logger).Log(
						"msg", "could not get CRDT log file information",
						"err", err,
					)
					os.Exit(1)
				}

				// Create a buffer of capacity of read file size.
				buf := bytes.NewBuffer(make([]byte, 0, (info.Size() - msgsSize)))

				// Reset position to byte after all sent messages in file.
				_, err = sender.updLog.Seek(msgsSize, os.SEEK_SET)
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
				// of CRDT log file, effectively deleting the
				// successfully sent bulk of messages.
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

				sender.lock.Unlock()

				// Renew timer.
				triggerT.Reset(triggerD)
			}
		}
	}
}
