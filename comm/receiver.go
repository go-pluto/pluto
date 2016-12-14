package comm

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	// "github.com/numbleroot/pluto/crdt"
)

// Structs

// Receiver bundles all information needed to accept
// and process incoming CRDT downstream messages.
type Receiver struct {
	lock             *sync.Mutex
	name             string
	msgInLog         chan bool
	socket           net.Listener
	writeLog         *os.File
	updLog           *os.File
	incVClock        chan string
	updVClock        chan map[string]int
	vclock           map[string]int
	applyCRDTUpdChan chan string
	doneCRDTUpdChan  chan bool
	nodes            []string
}

// Functions

// InitReceiver initializes above struct and sets
// default values. It starts involved background
// routines and send initial channel trigger.
func InitReceiver(name string, logFilePath string, socket net.Listener, applyCRDTUpdChan chan string, doneCRDTUpdChan chan bool, nodes []string) (chan string, chan map[string]int, error) {

	// Make a channel to communicate over with local
	// processes intending to process received messages.
	msgInLog := make(chan bool, 1)

	// Make a channel to return and be used in sender
	// to indicate a particular vector clock entry is
	// supposed to be incremented.
	incVClock := make(chan string)

	// Additionally, make a channel to send the updated
	// vector clock over after a successful increment.
	updVClock := make(chan map[string]int)

	// Create and initialize new struct.
	recv := &Receiver{
		lock:             new(sync.Mutex),
		name:             name,
		msgInLog:         msgInLog,
		socket:           socket,
		incVClock:        incVClock,
		updVClock:        updVClock,
		vclock:           make(map[string]int),
		applyCRDTUpdChan: applyCRDTUpdChan,
		doneCRDTUpdChan:  doneCRDTUpdChan,
		nodes:            nodes,
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

	// Start routine in background that takes care of
	// vector clock increments.
	go recv.IncVClockEntry()

	// Apply received messages in background.
	go recv.ApplyStoredMsgs()

	// If we just started the application, perform an
	// initial run to check if log file contains elements.
	recv.msgInLog <- true

	// Accept incoming messages in background.
	go recv.AcceptIncMsgs()

	return incVClock, updVClock, nil
}

// IncVClockEntry waits for an incoming name of a node on
// channel defined during initialization and passed on to
// senders. If the node is present in vector clock map, its
// value is incremented by one.
func (recv *Receiver) IncVClockEntry() {

	for {

		// Wait for name of node on channel.
		entry := <-recv.incVClock

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

			// Send back the updated vector clock on other
			// defined channel to sender.
			recv.updVClock <- updatedVClock
		}

		// Unlock struct.
		recv.lock.Unlock()
	}
}

// AcceptIncMsgs runs in background and waits for
// incoming CRDT messages. As soon as received, it
// dispatches into next routine.
func (recv *Receiver) AcceptIncMsgs() error {

	for {

		// Accept request or fail on error.
		conn, err := recv.socket.Accept()
		if err != nil {
			return fmt.Errorf("[comm.AcceptIncMsgs] Accepting incoming sync messages at %s failed with: %s\n", recv.name, err.Error())
		}

		// Dispatch into own goroutine.
		go recv.StoreIncMsgs(conn)
	}
}

// StoreIncMsgs takes received message string and saves
// it into incoming CRDT message log file.
func (recv *Receiver) StoreIncMsgs(conn net.Conn) {

	// Create new buffered reader for connection.
	r := bufio.NewReader(conn)

	// Read string until newline character is received.
	msgRaw, err := r.ReadString('\n')
	if err != nil {
		log.Fatalf("[comm.StoreIncMsgs] Error while reading sync message: %s\n", err.Error())
	}

	// Unless we do not receive the signal that continuous CRDT
	// message transmission is done, we accept new messages.
	for msgRaw != "> done <" {

		// Lock mutex.
		recv.lock.Lock()

		// Write it to message log file.
		_, err = recv.writeLog.WriteString(msgRaw)
		if err != nil {
			log.Fatalf("[comm.StoreIncMsgs] Writing to CRDT log file failed with: %s\n", err.Error())
		}

		// Save to stable storage.
		err = recv.writeLog.Sync()
		if err != nil {
			log.Fatalf("[comm.StoreIncMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
		}

		// Unlock mutex.
		recv.lock.Unlock()

		// Indicate to applying routine that a new message
		// is available to process.
		if len(recv.msgInLog) < 1 {
			recv.msgInLog <- true
		}

		// Read next CRDT message until newline character is received.
		msgRaw, err = r.ReadString('\n')
		if err != nil {
			log.Fatalf("[comm.StoreIncMsgs] Error while reading next sync message: %s\n", err.Error())
		}
	}
}

// ApplyStoredMsgs waits for a signal on a channel that
// indicates a new available message to process, reads and
// updates the CRDT log file and applies the payload to
// the CRDT state.
func (recv *Receiver) ApplyStoredMsgs() {

	for {

		// Wait for signal that new message was written to
		// log file so that we can process it.
		<-recv.msgInLog

		// Lock mutex.
		recv.lock.Lock()

		// Most of the following commands are taking from
		// this stackoverflow answer which describes a way
		// to pop the first line of a file and write back
		// the remaining parts:
		// http://stackoverflow.com/a/30948278
		info, err := recv.updLog.Stat()
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Could not get CRDT log file information: %s\n", err.Error())
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
			log.Fatalf("[comm.ApplyStoredMsgs] Error while retrieving current head position in CRDT log file: %s\n", err.Error())
		}

		// Calculate size of needed buffer.
		bufferSize := logSize - curOffset

		// Account for case when offset reached end of log file.
		if logSize == curOffset {

			log.Printf("[comm.ApplyStoredMsgs] Reached end of log file, resetting to beginning.\n")

			// Reset position to beginning of file.
			_, err = recv.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				log.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
			}

			// Unlock log file mutex.
			recv.lock.Unlock()

			// Send signal again to check for next log items.
			if len(recv.msgInLog) < 1 {
				recv.msgInLog <- true
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
			log.Fatalf("[comm.ApplyStoredMsgs] Could not copy CRDT log file contents to buffer: %s\n", err.Error())
		}

		// Read current message at head position from log file.
		msgRaw, err := buf.ReadString('\n')
		if (err != nil) && (err != io.EOF) {
			log.Fatalf("[comm.ApplyStoredMsgs] Error during extraction of first line in CRDT log file: %s\n", err.Error())
		}

		// Save length of just read message for later use.
		msgRawLength := int64(len(msgRaw))

		// Parse sync message.
		msg, err := Parse(msgRaw)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Error while parsing sync message: %s\n", err.Error())
		}

		// Initially, set apply indicator to true. This means,
		// that the message would be considered for further parsing.
		applyMsg := true

		// Check if this message is an already received or
		// the expected next one from the sending node.
		// If not, set indicator to false.
		if (msg.VClock[msg.Sender] != recv.vclock[msg.Sender]) &&
			(msg.VClock[msg.Sender] != (recv.vclock[msg.Sender] + 1)) {
			log.Printf("[comm.ApplyStoredMsgs] %s: applyMsg false because msg.clock[sender] = %d != %d = recv.clock[sender]\n", recv.name, msg.VClock[msg.Sender], recv.vclock[msg.Sender])
			applyMsg = false
		}

		for node, value := range msg.VClock {

			if node != msg.Sender {

				// Next, range over all received vector clock values
				// and check if they do not exceed the locally stored
				// values for these nodes.
				if value > recv.vclock[node] {
					log.Printf("[comm.ApplyStoredMsgs] %s: applyMsg false because 2\n", recv.name)
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
				done := <-recv.doneCRDTUpdChan

				log.Printf("[comm.ApplyStoredMsgs] node said %#v\n", done)

			} else {
				log.Printf("[comm.ApplyStoredMsgs] OLD message, duplicate: %s", msgRaw)
			}

			for node, value := range msg.VClock {

				// Adjust local vector clock to continue with pair-wise
				// maximum of the vector clock elements.
				if value > recv.vclock[node] {
					recv.vclock[node] = value
				}
			}

			// Reset head position to curOffset saved at beginning of loop.
			_, err = recv.updLog.Seek(curOffset, os.SEEK_SET)
			if err != nil {
				log.Fatal(err)
			}

			// Copy reduced buffer contents back to current position
			// of CRDT log file, effectively deleting the read line.
			newNumOfBytes, err := io.Copy(recv.updLog, buf)
			if err != nil {
				log.Fatalf("[comm.ApplyStoredMsgs] Error during copying buffer contents back to CRDT log file: %s\n", err.Error())
			}

			// Now, truncate log file size to (curOffset + newNumOfBytes),
			// reducing the file size by length of handled message.
			err = recv.updLog.Truncate((curOffset + newNumOfBytes))
			if err != nil {
				log.Fatalf("[comm.ApplyStoredMsgs] Could not truncate CRDT log file: %s\n", err.Error())
			}

			// Sync changes to stable storage.
			err = recv.updLog.Sync()
			if err != nil {
				log.Fatalf("[comm.ApplyStoredMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
			}

			// Reset position to beginning of file because the
			// chances are high that we now can proceed in order
			// of CRDT message log file.
			_, err = recv.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				log.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
			}
		} else {

			log.Printf("[comm.ApplyStoredMsgs] Message was out-of-order. Moving to next message in log file at position (curOffset + msgRawLength) = %d\n", (curOffset + msgRawLength))

			// Set position of head to byte after just read message,
			// effectively delaying execution of that message.
			_, err = recv.updLog.Seek((curOffset + msgRawLength), os.SEEK_SET)
			if err != nil {
				log.Fatalf("[comm.ApplyStoredMsgs] Error while moving position in CRDT log file to next line: %s\n", err.Error())
			}
		}

		// Unlock mutex.
		recv.lock.Unlock()

		// We do not know how many elements are waiting in the
		// log file. Therefore attempt to process next one and
		// if it does not exist, the loop iteration will abort.
		if len(recv.msgInLog) < 1 {
			recv.msgInLog <- true
		}
	}
}
