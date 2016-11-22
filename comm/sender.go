package comm

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"crypto/tls"
)

// Structs

// Sender bundles information needed for sending
// out sync messages via CRDTs.
type Sender struct {
	lock     *sync.Mutex
	name     string
	inc      chan string
	msgInLog chan bool
	vclock   map[string]int
	writeLog *os.File
	updLog   *os.File
	nodes    map[string]*tls.Conn
}

// Functions

// InitSender initializes above struct and sets
// default values for most involved elements to start
// with. It returns a channel local processes can put
// CRDT changes into, so that those changes will be
// communicated to connected nodes.
func InitSender(name string, logFilePath string, nodes map[string]*tls.Conn) (chan string, error) {

	// Make a channel to communicate over with
	// local processes intending to send a message
	// and one to signal that a new message was written
	// to log and is ready to be send out.
	inc := make(chan string)
	msgInLog := make(chan bool, 1)

	// Create and initialize what we need for
	// a CRDT sender routine.
	sender := &Sender{
		lock:     new(sync.Mutex),
		name:     name,
		inc:      inc,
		msgInLog: msgInLog,
		vclock:   make(map[string]int),
		nodes:    nodes,
	}

	// Open log file descriptor for writing.
	write, err := os.OpenFile(logFilePath, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	if err != nil {
		return nil, fmt.Errorf("[comm.InitSender] Opening CRDT log file for writing failed with: %s\n", err.Error())
	}
	sender.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("[comm.InitSender] Opening CRDT log file for updating failed with: %s\n", err.Error())
	}
	sender.updLog = upd

	// Initially set vector clock entries to 0.
	for i, _ := range nodes {
		sender.vclock[i] = 0
	}

	// Including the entry of this node.
	sender.vclock[name] = 0

	// Start brokering routine in background.
	go sender.BrokerMsgs()

	// Start sending routine in background.
	go sender.SendMsgs()

	// If we just started the application, perform an
	// initial run to check if log file contains elements.
	sender.msgInLog <- true

	// Return this channel to pass to processes.
	return inc, nil
}

// BrokerMsgs awaits a CRDT message to send to downstream
// replicas from one of the local processes on channel inc.
// It stores the message for sending in a dedicated CRDT log
// file and passes on a signal that a new message is available.
func (sender *Sender) BrokerMsgs() {

	for {

		// Receive CRDT payload to send to other nodes
		// on incoming channel.
		payload := <-sender.inc

		// Lock mutex.
		sender.lock.Lock()

		// Write it to message log file.
		_, err := sender.writeLog.WriteString(payload)
		if err != nil {
			log.Fatalf("[comm.BrokerMsgs] Writing to CRDT log file failed with: %s\n", err.Error())
		}

		// Save to stable storage.
		err = sender.writeLog.Sync()
		if err != nil {
			log.Fatalf("[comm.BrokerMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
		}

		// Unlock mutex.
		sender.lock.Unlock()

		log.Printf("[comm.BrokerMsgs] Wrote to CRDT log file: '%s'\n", payload)

		// Inidicate consecutive loop iterations
		// that a message is waiting in log.
		sender.msgInLog <- true
	}
}

// SendMsgs waits for a signal indicating that a message
// is waiting in the log file to be send out and sends that
// to all downstream nodes.
func (sender *Sender) SendMsgs() {

	for {

		// Wait for signal that new message was written to
		// log file so that we can send it out.
		<-sender.msgInLog

		// Lock mutex.
		sender.lock.Lock()

		// Most of the following commands are taking from
		// this stackoverflow answer which describes a way
		// to pop the first line of a file and write back
		// the remaining parts:
		// http://stackoverflow.com/a/30948278
		info, err := sender.updLog.Stat()
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Could not get CRDT log file information: %s\n", err.Error())
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
			log.Fatalf("[comm.SendMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
		}

		// Copy contents of log file to prepared buffer.
		_, err = io.Copy(buf, sender.updLog)
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Could not copy CRDT log file contents to buffer: %s\n", err.Error())
		}

		// Read oldest message from log file.
		payload, err := buf.ReadString('\n')
		if (err != nil) && (err != io.EOF) {
			log.Fatalf("[comm.SendMsgs] Error during extraction of first line in CRDT log file: %s\n", err.Error())
		}

		// Reset position to beginning of file.
		_, err = sender.updLog.Seek(0, os.SEEK_SET)
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
		}

		// Update this node's vector clock.
		sender.vclock[sender.name] += 1

		// Create a new message based on these values.
		msg := Message{
			vclock:  sender.vclock,
			payload: payload,
		}

		// Unlock mutex.
		sender.lock.Unlock()

		for i, conn := range sender.nodes {

			sent := 0
			marshalledMsg := msg.String()

			// Write message to TLS connections.
			_, err := fmt.Fprintf(conn, "%s\n", marshalledMsg)
			for err != nil {

				// If we tried to send the message three times
				// and had no success, log error and give up.
				// TODO: This is not the intended behaviour.
				if sent == 2 {
					log.Fatalf("[comm.SendMsgs] Tried to send out CRDT update to node %s three times without success. Giving up. TODO!\n", i)
				}

				// Log fail.
				log.Printf("[comm.SendMsgs] Sending CRDT update to node %s failed with: %s\n", i, err.Error())

				// Retry transfer.
				_, err = fmt.Fprintf(conn, "%s\n", marshalledMsg)

				// Increment break counter.
				sent++
			}
		}

		// Lock mutex.
		sender.lock.Lock()

		// Copy reduced buffer contents back to beginning
		// of CRDT log file, effectively deleting the first line.
		newNumOfBytes, err := io.Copy(sender.updLog, buf)
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Error during copying buffer contents back to CRDT log file: %s\n", err.Error())
		}

		// Now, truncate log file size to exact amount
		// of bytes copied from buffer.
		err = sender.updLog.Truncate(newNumOfBytes)
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Could not truncate CRDT log file: %s\n", err.Error())
		}

		// Sync changes to stable storage.
		err = sender.updLog.Sync()
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
		}

		// Reset position to beginning of file.
		_, err = sender.updLog.Seek(0, os.SEEK_SET)
		if err != nil {
			log.Fatalf("[comm.SendMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
		}

		// Unlock mutex.
		sender.lock.Unlock()

		// We do not know how many elements are waiting in the
		// log file. Therefore attempt to send next one and if
		// it does not exist, the loop iteration will abort.
		sender.msgInLog <- true
	}
}
