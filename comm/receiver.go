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
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Variables

// Separator for splitting meta data log
// file content at newline character.
var lineSep = []byte("\n")

// Separator for splitting each meta data
// log file line into start and end value
// for byte range they it describes.
var areaSep = []byte("-")

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
	logFilePath      string
	writeLog         *os.File
	metaFilePath     string
	metaLog          *os.File
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
func InitReceiver(logger log.Logger, name string, listenAddr string, publicAddr string, logFilePath string, metaFilePath string, vclockLogPath string, socket net.Listener, tlsConfig *tls.Config, applyCRDTUpdChan chan Msg, doneCRDTUpdChan chan struct{}, nodes map[string]string) (chan string, chan map[string]uint32, error) {

	recv := &Receiver{
		lock:             &sync.Mutex{},
		logger:           logger,
		name:             name,
		listenAddr:       listenAddr,
		publicAddr:       publicAddr,
		msgInLog:         make(chan struct{}, 1),
		socket:           socket,
		tlsConfig:        tlsConfig,
		logFilePath:      logFilePath,
		metaFilePath:     metaFilePath,
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
		return nil, nil, fmt.Errorf("opening CRDT log file for writing append-only failed with: %v", err)
	}
	recv.writeLog = write

	// Open log file for tracking meta data about already
	// applied parts of the CRDT update messages log file.
	meta, err := os.OpenFile(metaFilePath, (os.O_CREATE | os.O_RDWR), 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("opening meta data log file of applied CRDT update messages failed with: %v", err)
	}
	recv.metaLog = meta

	// Initially, reset position in meta file to beginning.
	_, err = recv.metaLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, nil, fmt.Errorf("could not reset position in meta data log file: %v", err)
	}

	// Initially, set vector clock entries to 0.
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

	// Create a timer that waits for the specified
	// amount of seconds to elapse and then fires.
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
func (recv *Receiver) Incoming(ctx context.Context, binMsgs *BinMsgs) (*Conf, error) {

	recv.lock.Lock()

	// Append bulk of messages to message log file.
	_, err := recv.writeLog.Write(binMsgs.Data)
	if err != nil {
		return nil, err
	}

	// Save to stable storage.
	err = recv.writeLog.Sync()
	if err != nil {
		return nil, err
	}

	recv.lock.Unlock()

	// Indicate to applying routine that a new message
	// is available to process.
	if len(recv.msgInLog) < 1 {
		recv.msgInLog <- struct{}{}
	}

	return &Conf{
		Status: 0,
	}, nil
}

// ApplyStoredMsgs waits for a signal on a channel that
// indicates a new available message to process, reads and
// updates the CRDT log file and applies the payload to
// the CRDT state.
func (recv *Receiver) ApplyStoredMsgs() {

	for {

		// level.Debug(recv.logger).Log("msg", "attempting to apply stored messages")

		recv.lock.Lock()

		// Read the whole current content of the CRDT update
		// messages log file to have it present in memory.
		data, err := ioutil.ReadFile(recv.logFilePath)
		if err != nil {

			recv.lock.Unlock()

			level.Error(recv.logger).Log(
				"msg", "failed to read whole CRDT update messages log file",
				"err", err,
			)
			continue
		}

		recv.lock.Unlock()

		// If there currently is no content available to apply,
		// sleep shortly and skip to next iteration.
		if len(data) == 0 {
			// level.Debug(recv.logger).Log("msg", "CRDT update messages log file empty, skipping run")
			time.Sleep(1 * time.Second)
			continue
		}

		// Reset position in meta data file to beginning again.
		_, err = recv.metaLog.Seek(0, os.SEEK_SET)
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "could not reset position in meta data log file",
				"err", err,
			)
			os.Exit(1)
		}

		// Read the whole content of the meta data log file
		// that stores the CRDT file areas we already applied.
		metaRaw, err := ioutil.ReadFile(recv.metaFilePath)
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "failed to read whole meta data log file",
				"err", err,
			)
			continue
		}

		meta := make([]map[string]int64, 0)

		if len(metaRaw) > 0 {

			// Split the meta data file at newline.
			metaLines := bytes.Split(metaRaw, lineSep)

			// Prepare slice of map of start and end positions.
			meta = make([]map[string]int64, len(metaLines))

			for i := range metaLines {

				var err error
				meta[i] = make(map[string]int64)

				// Split current area at hyphen that separates
				// start from end range value.
				area := bytes.Split(metaLines[i], areaSep)

				// Parse byte representation of start value into int64.
				meta[i]["start"], err = strconv.ParseInt(string(area[0]), 10, 64)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "failed to convert string to int indicating start of current area",
						"err", err,
					)
					os.Exit(1)
				}

				// Parse byte representation of end value into int64.
				meta[i]["end"], err = strconv.ParseInt(string(area[1]), 10, 64)
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "failed to convert string to int indicating end of current area",
						"err", err,
					)
					os.Exit(1)
				}

				level.Debug(recv.logger).Log(
					"msg", "extracted start and end of area",
					"metaLines[i]", metaLines[i],
					"start", meta[i]["start"],
					"end", meta[i]["end"],
				)
			}
		}

		fenceItem := map[string]int64{
			"start": int64(len(data)),
			"end":   int64(len(data)),
		}

		meta = append(meta, fenceItem)

		// fmt.Printf("BEGIN meta: '%v'\n", meta)

		done := false
		for !done {

			// Initially, assume there is not one
			// message anymore to potentially apply.
			noneLeft := true

			// Initially, assume we were not able to
			// find one applicable message.
			noneApplied := true

			for i := 0; i < len(meta); i++ {

				// fmt.Printf("len(meta) = %d\n", len(meta))

				// If we currently look at the very first
				// area, directly skip to the next one.
				if meta[i]["start"] == 0 {
					continue
				}

				// fmt.Println("still here?")

				// Calculate difference between start of
				// current area and end of last one.
				var lastEnd int64
				if i == 0 {
					lastEnd = 0
				} else {
					lastEnd = meta[(i - 1)]["end"]
				}
				diffStartEnd := meta[i]["start"] - lastEnd

				// If areas are contiguous, continue
				// with next area.
				if diffStartEnd < 1 {
					continue
				}

				// Gaps mean that there are still messages
				// we might be able to apply. Indicate this.
				noneLeft = false

				// Track how many bytes of the current area
				// we have already considered.
				var consideredBytes int64 = 0

				for (lastEnd + consideredBytes) != meta[i]["start"] {

					// fmt.Printf("(lastEnd + consideredBytes) ?= meta[%d][\"start\"]   =>   (%d + %d) = %d ?= %d\n", i, lastEnd, consideredBytes, (lastEnd + consideredBytes), meta[i]["start"])

					// We found a potentially applicable message.
					// Overlay current position from data slice with buffer.
					// . . . x x x x MESSAGE? x x x x . . .
					// <-------last| |-read-| |cur-------->
					buf := bytes.NewBuffer(data[(lastEnd + consideredBytes):meta[i]["start"]])

					// Read first bytes after last message to find
					// byte number of enclosed ProtoBuf message.
					numBytesRaw, err := buf.ReadBytes(';')
					if (err != nil) && (err != io.EOF) {
						level.Error(recv.logger).Log(
							"msg", "error extracting number of bytes of considered message in CRDT log file",
							"err", err,
						)
						os.Exit(1)
					}

					// Convert string to int64.
					numBytes, err := strconv.ParseInt((string(numBytesRaw[:(len(numBytesRaw) - 1)])), 10, 64)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "failed to convert string to int indicating number of bytes",
							"err", err,
						)
						os.Exit(1)
					}

					// Calculate total space of stored message:
					// number of bytes prefix + ';' + actual message.
					msgSize := int64(len(numBytesRaw)) + numBytes

					// Extract marshalled ProtoBuf message area from data.
					// . . . x x x x 4 1 6 ; P R O T O B U F M S G x x x x . . .
					// <-------last| |len|   |--------read-------| |cur-------->
					msgRaw := data[(lastEnd + consideredBytes + int64(len(numBytesRaw))):(lastEnd + consideredBytes + msgSize)]

					// Attempt to unmarshal extracted data area
					// into a ProtoBuf message.
					msg := &Msg{}
					err = proto.Unmarshal(msgRaw, msg)
					if err != nil {
						level.Error(recv.logger).Log(
							"msg", "failed to unmarshal considered ProtoBuf message into defined Msg struct",
							"err", err,
						)
						os.Exit(1)
					}

					// Initially, set apply indicator to true. This means, that
					// the message would be considered for further parsing.
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
							// and check that they do not exceed the locally stored
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

						noneApplied = false

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

						// Mark area as applied by inserting an
						// entry into the areas structure.
						meta = append(meta, make(map[string]int64))
						copy(meta[(i+1):], meta[i:])
						meta[i] = make(map[string]int64)
						meta[i]["start"] = (lastEnd + consideredBytes)
						meta[i]["end"] = (lastEnd + consideredBytes + msgSize)

						// Update number of considered bytes of the
						// currently inspected area.
						consideredBytes += msgSize

						// fmt.Printf("END meta: '%v'\n", meta)

						i++

						// level.Debug(recv.logger).Log("msg", "done with receiver main")

					} else {

						level.Warn(recv.logger).Log("msg", "message was out of order, taking next one")

						// TODO: Remove.
						time.Sleep(3 * time.Second)
					}
				}
			}

			if (noneLeft == true) || (noneApplied == true) {
				// fmt.Println("noneLeft or noneApplied => done true")
				done = true
			}
		}

		// If two or more items are available in meta,
		// check whether we can merge the areas.
		if len(meta) > 1 {

			for i := 1; i < len(meta); i++ {

				if meta[(i - 1)]["end"] == meta[i]["start"] {
					meta[(i - 1)]["end"] = meta[i]["end"]
					meta = append(meta[:i], meta[(i+1):]...)
					i--
				}
			}
		}

		// fmt.Printf("MERGED meta: %v\n", meta)

		metaLinesBytes := make([][]byte, len(meta))

		for i := range meta {
			metaLinesBytes[i] = []byte(fmt.Sprintf("%d-%d", meta[i]["start"], meta[i]["end"]))
		}

		metaBytes := bytes.Join(metaLinesBytes, []byte("\n"))

		// Reset position in meta file to beginning.
		_, err = recv.metaLog.Seek(0, os.SEEK_SET)
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "could not reset position in meta data log file",
				"err", err,
			)
			os.Exit(1)
		}

		// Write-back updated meta data to log file.
		writtenBytes, err := recv.metaLog.Write(metaBytes)
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "writing to meta data log failed",
				"err", err,
			)
			os.Exit(1)
		}

		// Truncate file to amount of bytes written.
		err = recv.metaLog.Truncate(int64(writtenBytes))
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "failed to truncate meta data log file to new size",
				"err", err,
			)
			os.Exit(1)
		}

		// Reset position in meta file to beginning.
		_, err = recv.metaLog.Seek(0, os.SEEK_SET)
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "could not reset position in meta data log file",
				"err", err,
			)
			os.Exit(1)
		}

		// Sync meta data log file to stable storage.
		err = recv.metaLog.Sync()
		if err != nil {
			level.Error(recv.logger).Log(
				"msg", "could not sync meta data log file to stable storage",
				"err", err,
			)
			os.Exit(1)
		}

		// TODO: Remove.
		time.Sleep(1 * time.Second)
	}
}
