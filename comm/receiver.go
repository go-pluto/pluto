package comm

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"io/ioutil"
)

// Structs

// Receiver bundles all information needed to accept
// and process incoming CRDT downstream messages.
type Receiver struct {
	lock             *sync.Mutex
	name             string
	msgInLog         chan struct{}
	socket           net.Listener
	writeLog         *os.File
	updLog           *os.File
	incVClock        chan string
	updVClock        chan map[string]int
	vclock           map[string]int
	vclockLog        *os.File
	applyCRDTUpdChan chan string
	doneCRDTUpdChan  chan struct{}
	nodes            []string
	wg               *sync.WaitGroup
	shutdown         chan struct{}
}

// Functions

// InitReceiver initializes above struct and sets
// default values. It starts involved background
// routines and send initial channel trigger.
func InitReceiver(name string, logFilePath string, vclockLogPath string, socket net.Listener, applyCRDTUpdChan chan string, doneCRDTUpdChan chan struct{}, downRecv chan struct{}, nodes []string) (chan string, chan map[string]int, error) {

	// Create and initialize new struct.
	recv := &Receiver{
		lock:             new(sync.Mutex),
		name:             name,
		msgInLog:         make(chan struct{}, 1),
		socket:           socket,
		incVClock:        make(chan string),
		updVClock:        make(chan map[string]int),
		vclock:           make(map[string]int),
		applyCRDTUpdChan: applyCRDTUpdChan,
		doneCRDTUpdChan:  doneCRDTUpdChan,
		nodes:            nodes,
		wg:               new(sync.WaitGroup),
		shutdown:         make(chan struct{}, 3),
	}

	// Open log file descriptor for writing.
	write, err := os.OpenFile(logFilePath, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("[comm.InitReceiver] Opening CRDT log file for writing failed with: %s\n", err.Error())
	}
	recv.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("[comm.InitReceiver] Opening CRDT log file for updating failed with: %s\n", err.Error())
	}
	recv.updLog = upd

	// Initially, reset position in update file to beginning.
	_, err = recv.updLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, nil, fmt.Errorf("[comm.InitReceiver] Could not reset position in update CRDT log file: %s\n", err.Error())
	}

	// Initially set vector clock entries to 0.
	for _, node := range nodes {
		recv.vclock[node] = 0
	}

	// Including the entry of this node.
	recv.vclock[name] = 0

	// Open log file of last known vector clock values.
	vclockLog, err := os.OpenFile(vclockLogPath, (os.O_CREATE | os.O_RDWR), 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("[comm.InitReceiver] Opening vector clock log failed with: %s\n", err.Error())
	}
	recv.vclockLog = vclockLog

	// Initially, reset position in vector clock file to beginning.
	_, err = recv.vclockLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, nil, fmt.Errorf("[comm.InitReceiver] Could not reset position in vector clock log: %s\n", err.Error())
	}

	// If vector clock entries were preserved, set them.
	err = recv.SetVClockEntries()
	if err != nil {
		return nil, nil, fmt.Errorf("[comm.InitReceiver] Reading in stored vector clock entries failed: %s\n", err.Error())
	}

	// Start eventual shutdown routine in background.
	go recv.Shutdown(downRecv)

	// Start routine in background that takes care of
	// vector clock increments.
	recv.wg.Add(1)
	go recv.IncVClockEntry()

	// Apply received messages in background.
	recv.wg.Add(1)
	go recv.ApplyStoredMsgs()

	// If we just started the application, perform an
	// initial run to check if log file contains elements.
	recv.msgInLog <- struct{}{}

	// Start triggering msgInLog events periodically.
	recv.wg.Add(1)
	go recv.TriggerMsgApplier()

	// Accept incoming messages in background.
	recv.wg.Add(1)
	go recv.AcceptIncMsgs()

	return recv.incVClock, recv.updVClock, nil
}

// SetVClockEntries fetches saved vector clock entries
// from log file and sets them in internal vector clock.
// It expects to be the only goroutine currently operating
// on receiver.
func (recv *Receiver) SetVClockEntries() error {

	// Read all log contents.
	storedVClockBytes, err := ioutil.ReadAll(recv.vclockLog)
	if err != nil {
		return err
	}
	storedVClock := string(storedVClockBytes)

	// If log was empty (e.g., initially), return
	// success because we do not have anything to set.
	if storedVClock == "" {
		return nil
	}

	// Otherwise, split at semicola.
	pairs := strings.Split(string(storedVClock), ";")

	for _, pair := range pairs {

		// Split pairs at colon.
		entry := strings.Split(pair, ":")

		// Convert entry string to int.
		entryNumber, err := strconv.Atoi(entry[1])
		if err != nil {
			return err
		}

		// Set elements in vector clock of receiver.
		recv.vclock[entry[0]] = entryNumber
	}

	return nil
}

// SaveVClockEntries writes current status of vector
// clock to log file to recover from later. It expects to
// be the only goroutine currently operating on receiver.
func (recv *Receiver) SaveVClockEntries() error {

	vclockString := ""

	// Construct string of current vector clock.
	for node, entry := range recv.vclock {

		if vclockString == "" {
			vclockString = fmt.Sprintf("%s:%d", node, entry)
		} else {
			vclockString = fmt.Sprintf("%s;%s:%d", vclockString, node, entry)
		}
	}

	// Over-write old vector clock log. Reset position
	// of read-write head to beginning.
	_, err := recv.vclockLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	// Write vclock string to file.
	newNumOfBytes, err := recv.vclockLog.WriteString(vclockString)
	if err != nil {
		return nil
	}

	// Truncate file to just written content.
	err = recv.vclockLog.Truncate(int64(newNumOfBytes))
	if err != nil {
		return nil
	}

	return nil
}

// Shutdown awaits a receiver global shutdown signal and
// in turn instructs involved goroutines to finish and
// clean up.
func (recv *Receiver) Shutdown(downRecv chan struct{}) {

	// Wait for signal.
	<-downRecv

	stdlog.Printf("[comm.Shutdown] Receiver: shutting down...\n")

	// Instruct other goroutines to shutdown.
	recv.shutdown <- struct{}{}
	recv.shutdown <- struct{}{}
	recv.shutdown <- struct{}{}
	recv.shutdown <- struct{}{}

	// Close involved channels.
	close(recv.incVClock)
	close(recv.updVClock)
	close(recv.msgInLog)

	// Wait for both to indicate finish.
	recv.wg.Wait()

	// Close receiving socket.
	recv.lock.Lock()
	recv.socket.Close()
	recv.lock.Unlock()

	stdlog.Printf("[comm.Shutdown] Receiver: done!\n")
}

// IncVClockEntry waits for an incoming name of a node on
// channel defined during initialization and passed on to
// senders. If the node is present in vector clock map, its
// value is incremented by one.
func (recv *Receiver) IncVClockEntry() {

	for {

		select {

		// Check if a shutdown signal was sent.
		case <-recv.shutdown:

			// Call done handler of wait group for this
			// routine on exiting this function.
			defer recv.wg.Done()
			return

		default:

			// Wait for name of node on channel.
			entry, ok := <-recv.incVClock

			if ok {

				// Lock receiver struct.
				recv.lock.Lock()

				// Check if received node name exists in map.
				if _, exists := recv.vclock[entry]; exists {

					// If it does, increment its vector clock
					// value by one.
					recv.vclock[entry] += 1

					// Make a deep copy of current vector clock
					// map to pass back via channel to sender.
					updatedVClock := make(map[string]int)
					for node, value := range recv.vclock {
						updatedVClock[node] = value
					}

					// Save updated vector clock to log file.
					err := recv.SaveVClockEntries()
					if err != nil {
						stdlog.Fatalf("[comm.IncVClockEntry] Saving updated vector clock to file failed: %s\n", err.Error())
					}

					// Send back the updated vector clock on other
					// defined channel to sender.
					recv.updVClock <- updatedVClock
				}

				// Unlock struct.
				recv.lock.Unlock()
			}
		}
	}
}

// AcceptIncMsgs runs in background and waits for
// incoming CRDT messages. As soon as received, it
// dispatches into next routine.
func (recv *Receiver) AcceptIncMsgs() error {

	// Call done handler of wait group for this
	// routine on exiting this function.
	defer recv.wg.Done()

	for {

		select {

		// Check if a shutdown signal was sent.
		case <-recv.shutdown:

			// Close file descriptor.
			recv.lock.Lock()
			recv.writeLog.Close()
			recv.lock.Unlock()

			return nil

		default:

			// Accept request or fail on error.
			conn, err := recv.socket.Accept()
			if err != nil {
				return fmt.Errorf("[comm.AcceptIncMsgs] Accepting incoming sync messages at %s failed with: %s\n", recv.name, err.Error())
			}

			go recv.StoreIncMsgs(conn)
		}
	}
}

// TriggerMsgApplier starts a timer that triggers
// an msgInLog event when duration elapsed. Supposed
// to routinely poke the ApplyStoredMsgs into checking
// for unprocessed messages in log.
func (recv *Receiver) TriggerMsgApplier() {

	// Specify duration to wait between triggers.
	triggerD := 5 * time.Second

	// Create a timer that waits for one second
	// to elapse and then fires.
	triggerT := time.NewTimer(triggerD)

	for {

		select {

		// Check if a shutdown signal was sent.
		case <-recv.shutdown:

			// Call done handler of wait group for this
			// routine on exiting this function.
			defer recv.wg.Done()
			return

		case <-triggerT.C:

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

// StoreIncMsgs takes received message string and saves
// it into incoming CRDT message log file.
func (recv *Receiver) StoreIncMsgs(conn net.Conn) {

	var err error

	// Initial value for received message in order
	// to skip past the mandatory ping message.
	msgRaw := "> ping <"

	// Create new buffered reader for connection.
	r := bufio.NewReader(conn)

	for msgRaw == "> ping <" {

		// Read string until newline character is received.
		msgRaw, err = r.ReadString('\n')
		if err != nil {

			if err.Error() == "EOF" {
				stdlog.Printf("[comm.StoreIncMsgs] Reading from closed connection. Ignoring.\n")
				return
			}

			stdlog.Fatalf("[comm.StoreIncMsgs] Error while reading sync message: %s\n", err.Error())
		}

		// Remove trailing characters denoting line end.
		msgRaw = strings.TrimRight(msgRaw, "\r\n")
	}

	// Lock mutex.
	recv.lock.Lock()

	// Write it to message log file.
	_, err = recv.writeLog.WriteString(msgRaw)
	if err != nil {
		stdlog.Fatalf("[comm.StoreIncMsgs] Writing to CRDT log file failed with: %s\n", err.Error())
	}

	// Append a newline symbol to just written line.
	newline := []byte("\n")
	_, err = recv.writeLog.Write(newline)
	if err != nil {
		stdlog.Fatalf("[comm.StoreIncMsgs] Appending a newline symbol to CRDT log file failed with: %s\n", err.Error())
	}

	// Save to stable storage.
	err = recv.writeLog.Sync()
	if err != nil {
		stdlog.Fatalf("[comm.StoreIncMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
	}

	// Unlock mutex.
	recv.lock.Unlock()

	// Indicate to applying routine that a new message
	// is available to process.
	if len(recv.msgInLog) < 1 {
		recv.msgInLog <- struct{}{}
	}
}

// ApplyStoredMsgs waits for a signal on a channel that
// indicates a new available message to process, reads and
// updates the CRDT log file and applies the payload to
// the CRDT state.
func (recv *Receiver) ApplyStoredMsgs() {

	for {

		select {

		// Check if a shutdown signal was sent.
		case <-recv.shutdown:

			// If so, close file handler.
			recv.lock.Lock()
			recv.updLog.Close()
			recv.lock.Unlock()

			// Call done handler of wait group for this
			// routine on exiting this function.
			defer recv.wg.Done()
			return

		default:

			// Wait for signal that new message was written to
			// log file so that we can process it.
			_, ok := <-recv.msgInLog

			if ok {

				// Lock mutex.
				recv.lock.Lock()

				// Most of the following commands are taking from
				// this stackoverflow answer which describes a way
				// to pop the first line of a file and write back
				// the remaining parts:
				// http://stackoverflow.com/a/30948278
				info, err := recv.updLog.Stat()
				if err != nil {
					stdlog.Fatalf("[comm.ApplyStoredMsgs] Could not get CRDT log file information: %s\n", err.Error())
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
					stdlog.Fatalf("[comm.ApplyStoredMsgs] Error while retrieving current head position in CRDT log file: %s\n", err.Error())
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
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
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
					stdlog.Fatalf("[comm.ApplyStoredMsgs] Could not copy CRDT log file contents to buffer: %s\n", err.Error())
				}

				// Read current message at head position from log file.
				msgRaw, err := buf.ReadString('\n')
				if (err != nil) && (err != io.EOF) {
					stdlog.Fatalf("[comm.ApplyStoredMsgs] Error during extraction of first line in CRDT log file: %s\n", err.Error())
				}

				// Save length of just read message for later use.
				msgRawLength := int64(len(msgRaw))

				// Parse sync message.
				msg, err := Parse(msgRaw)
				if err != nil {
					stdlog.Fatalf("[comm.ApplyStoredMsgs] Error while parsing sync message: %s\n", err.Error())
				}

				// Initially, set apply indicator to true. This means,
				// that the message would be considered for further parsing.
				applyMsg := true

				// Check if this message is an already received or
				// the expected next one from the sending node.
				// If not, set indicator to false.
				if (msg.VClock[msg.Sender] != recv.vclock[msg.Sender]) &&
					(msg.VClock[msg.Sender] != (recv.vclock[msg.Sender] + 1)) {
					applyMsg = false
				}

				for node, value := range msg.VClock {

					if node != msg.Sender {

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
					if msg.VClock[msg.Sender] == (recv.vclock[msg.Sender] + 1) {

						// Pass payload for higher-level interpretation
						// to channel connected to node.
						recv.applyCRDTUpdChan <- msg.Payload

						// Wait for done signal from node.
						<-recv.doneCRDTUpdChan
					}

					for node, value := range msg.VClock {

						// Adjust local vector clock to continue with pair-wise
						// maximum of the vector clock elements.
						if value > recv.vclock[node] {
							recv.vclock[node] = value
						}
					}

					// Save updated vector clock to log file.
					err := recv.SaveVClockEntries()
					if err != nil {
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Saving updated vector clock to file failed: %s\n", err.Error())
					}

					// Reset head position to curOffset saved at beginning of loop.
					_, err = recv.updLog.Seek(curOffset, os.SEEK_SET)
					if err != nil {
						stdlog.Fatal(err)
					}

					// Copy reduced buffer contents back to current position
					// of CRDT log file, effectively deleting the read line.
					newNumOfBytes, err := io.Copy(recv.updLog, buf)
					if err != nil {
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Error during copying buffer contents back to CRDT log file: %s\n", err.Error())
					}

					// Now, truncate log file size to (curOffset + newNumOfBytes),
					// reducing the file size by length of handled message.
					err = recv.updLog.Truncate((curOffset + newNumOfBytes))
					if err != nil {
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Could not truncate CRDT log file: %s\n", err.Error())
					}

					// Sync changes to stable storage.
					err = recv.updLog.Sync()
					if err != nil {
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
					}

					// Reset position to beginning of file because the
					// chances are high that we now can proceed in order
					// of CRDT message log file.
					_, err = recv.updLog.Seek(0, os.SEEK_SET)
					if err != nil {
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
					}
				} else {

					stdlog.Printf("[comm.ApplyStoredMsgs] Message was out of order. Next.\n")

					// Set position of head to byte after just read message,
					// effectively delaying execution of that message.
					_, err = recv.updLog.Seek((curOffset + msgRawLength), os.SEEK_SET)
					if err != nil {
						stdlog.Fatalf("[comm.ApplyStoredMsgs] Error while moving position in CRDT log file to next line: %s\n", err.Error())
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
