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

	"github.com/numbleroot/pluto/crdt"
)

// Structs

// Receiver bundles all information needed to accept
// and process incoming CRDT downstream messages.
type Receiver struct {
	lock      *sync.Mutex
	name      string
	msgInLog  chan bool
	socket    net.Listener
	writeLog  *os.File
	updLog    *os.File
	incVClock chan string
	updVClock chan map[string]int
	vclock    map[string]int
	nodes     []string
}

// Functions

// InitReceiver initializes above struct and sets
// default values. It starts involved background
// routines and send initial channel trigger.
func InitReceiver(name string, logFilePath string, socket net.Listener, nodes []string) (chan string, chan map[string]int, error) {

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
		lock:      new(sync.Mutex),
		name:      name,
		msgInLog:  msgInLog,
		socket:    socket,
		incVClock: incVClock,
		updVClock: updVClock,
		vclock:    make(map[string]int),
		nodes:     nodes,
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

// InitReceiverForeground initializes above struct
// and sets default values. It returns one half of
// the needed background routines and returns the
// receiver struct so that the others can be started
// in foreground.
func InitReceiverForeground(name string, logFilePath string, socket net.Listener, nodes []string) (*Receiver, chan string, chan map[string]int, error) {

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
		lock:      new(sync.Mutex),
		name:      name,
		msgInLog:  msgInLog,
		socket:    socket,
		incVClock: incVClock,
		updVClock: updVClock,
		vclock:    make(map[string]int),
		nodes:     nodes,
	}

	// Open log file descriptor for writing.
	write, err := os.OpenFile(logFilePath, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("[comm.InitReceiver] Opening CRDT log file for writing failed with: %s\n", err.Error())
	}
	recv.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("[comm.InitReceiver] Opening CRDT log file for updating failed with: %s\n", err.Error())
	}
	recv.updLog = upd

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

	return recv, incVClock, updVClock, nil
}

// IncVClockEntry waits for an incoming name of a node on
// channel defined during initialization and passed on to
// senders. If the node is present in vector clock map, its
// value is incremented by one.
func (recv *Receiver) IncVClockEntry() {

	for {

		log.Printf("[inc at %s] waiting for inc...", recv.name)

		// Wait for name of node on channel.
		entry := <-recv.incVClock

		// Lock receiver struct.
		recv.lock.Lock()

		// Check if received node name exists in map.
		if _, exists := recv.vclock[entry]; exists {

			log.Printf("[inc] old vclock value for %s: %d\n", entry, recv.vclock[entry])

			// If it does, increment its vector clock
			// value by one.
			recv.vclock[entry] += 1

			log.Printf("[inc] new vclock value for %s: %d\n", entry, recv.vclock[entry])

			// Send back the updated vector clock on other
			// defined channel to sender.
			recv.updVClock <- recv.vclock
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

		log.Printf("[comm.StoreIncMsgs] Wrote to CRDT log file: %s", msgRaw)

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

		// Check if log file is empty and continue at next
		// for loop iteration if that is the case.
		if info.Size() == 0 {
			recv.lock.Unlock()
			continue
		}

		// Create a buffer of capacity of read file size.
		buf := bytes.NewBuffer(make([]byte, 0, info.Size()))

		// Reset position to beginning of file.
		_, err = recv.updLog.Seek(0, os.SEEK_SET)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
		}

		// Copy contents of log file to prepared buffer.
		_, err = io.Copy(buf, recv.updLog)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Could not copy CRDT log file contents to buffer: %s\n", err.Error())
		}

		// Read oldest message from log file.
		msgRaw, err := buf.ReadString('\n')
		if (err != nil) && (err != io.EOF) {
			log.Fatalf("[comm.ApplyStoredMsgs] Error during extraction of first line in CRDT log file: %s\n", err.Error())
		}

		// Reset position to beginning of file.
		_, err = recv.updLog.Seek(0, os.SEEK_SET)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
		}

		// Parse sync message.
		msg, err := Parse(msgRaw)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Error while parsing sync message: %s\n", err.Error())
		}

		// TODO: Check vector clock validity and update appropriately.
		log.Printf("[comm.ApplyStoredMsgs] Should apply vector clock magic here: %v\n", msg.VClock)

		// Parse contained CRDT update message.
		msgCRDT, err := crdt.Parse(msg.Payload)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Error while parsing CRDT update message: %s\n", err.Error())
		}

		// TODO: Apply CRDT state.
		log.Printf("[comm.ApplyStoredMsgs] Should apply following CRDT state here: %v\n", msgCRDT.Arguments)

		// Copy reduced buffer contents back to beginning
		// of CRDT log file, effectively deleting the first line.
		newNumOfBytes, err := io.Copy(recv.updLog, buf)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Error during copying buffer contents back to CRDT log file: %s\n", err.Error())
		}

		// Now, truncate log file size to exact amount
		// of bytes copied from buffer.
		err = recv.updLog.Truncate(newNumOfBytes)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Could not truncate CRDT log file: %s\n", err.Error())
		}

		// Sync changes to stable storage.
		err = recv.updLog.Sync()
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
		}

		// Reset position to beginning of file.
		_, err = recv.updLog.Seek(0, os.SEEK_SET)
		if err != nil {
			log.Fatalf("[comm.ApplyStoredMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
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
