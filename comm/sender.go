package comm

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"crypto/tls"
)

// Structs

// Sender bundles information needed for sending
// out sync messages via CRDTs.
type Sender struct {
	name     string
	inc      chan string
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
func InitSender(name string, logFilePath string, nodes map[string]*tls.Conn) chan string {

	// Make a channel to communicate over with
	// local processes intending to send a message.
	inc := make(chan string)

	// Create and initialize what we need for
	// a CRDT sender routine.
	sender := &Sender{
		name:   name,
		inc:    inc,
		vclock: make(map[string]int),
		nodes:  nodes,
	}

	// Open log file descriptor for writing.
	write, err := os.OpenFile(logFilePath, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	if err != nil {
		log.Fatalf("[comm.InitSender] Opening CRDT log file for writing failed with: %s\n", err.Error())
	}
	sender.writeLog = write

	// Open log file descriptor for updating.
	upd, err := os.OpenFile(logFilePath, os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("[comm.InitSender] Opening CRDT log file for updating failed with: %s\n", err.Error())
	}
	sender.updLog = upd

	// Initially set vector clock entries to 0.
	for i, node := range nodes {
		log.Printf("INIT: i: %s, node: %v\n", i, node)

		sender.vclock[i] = 0
	}

	// Including the entry of this node.
	sender.vclock[name] = 0

	// Start receiving routine in background.
	go sender.HandleMessages()

	// Return this channel to pass to processes.
	return inc
}

// HandleMessages waits for messages on prior created
// channel, increments this node's vector clock entry
// and sends out a marshalled version of the whole message.
func (sender *Sender) HandleMessages() {

	// Channel that indicates whether a new message is in log
	// file since last iteration of for loop.
	msgInLog := make(chan bool, 1)

	// On start up, check for messages in log.
	msgInLog <- true

	for {

		select {

		case _ = <-msgInLog:

			log.Println("Message in log or start up")

			// Most of the following commands are taking from
			// this stackoverflow answer which describes a way
			// to pop the first line of a file and write back
			// the remaining parts:
			// http://stackoverflow.com/a/30948278
			info, err := sender.updLog.Stat()
			if err != nil {
				log.Fatalf("[comm.HandleMessages] Could not get CRDT log file information: %s\n", err.Error())
			}

			// Check if log file is empty and continue at next
			// for loop iteration if that is the case.
			if info.Size() == 0 {
				continue
			}

			// Create a buffer of capacity of read file size.
			buf := bytes.NewBuffer(make([]byte, 0, info.Size()))

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				log.Fatalf("[comm.HandleMessages] Could not reset position in CRDT log file: %s\n", err.Error())
			}

			// Copy contents of log file to prepared buffer.
			_, err = io.Copy(buf, sender.updLog)
			if err != nil {
				log.Fatalf("[comm.HandleMessages] Could not copy CRDT log file contents to buffer: %s\n", err.Error())
			}

			// Read oldest message from log file.
			payload, err := buf.ReadString('\n')
			if (err != nil) && (err != io.EOF) {
				log.Fatalf("[comm.HandleMessages] Error during extraction of first line in CRDT log file: %s\n", err.Error())
			}

			// Reset position to beginning of file.
			_, err = sender.updLog.Seek(0, os.SEEK_SET)
			if err != nil {
				log.Fatalf("[comm.HandleMessages] Could not reset position in CRDT log file: %s\n", err.Error())
			}

			// Update this node's vector clock.
			sender.vclock[sender.name] += 1

			// Create a new message based on these values.
			msg := Message{
				vclock:  sender.vclock,
				payload: payload,
			}

			for i, conn := range sender.nodes {

				log.Printf("SEND: i: %s, conn: %v\n", i, conn)

				sent := 0
				marshalledMsg := msg.String()

				// Write message to TLS connections.
				_, err := fmt.Fprintf(conn, "%s\n", marshalledMsg)
				for err != nil {

					// If we tried to send the message three times
					// and had no success, log error and give up.
					// TODO: This is not the intended behaviour.
					if sent == 2 {
						log.Fatalf("[comm.HandleMessages] Tried to send out CRDT update to node %s three times without success. Giving up. TODO!\n", i)
					}

					// Log fail.
					log.Printf("[comm.HandleMessages] Sending CRDT update to node %s failed with: %s\n", i, err.Error())

					// Retry transfer.
					_, err = fmt.Fprintf(conn, "%s\n", marshalledMsg)

					// Increment break counter.
					sent++
				}

				// Copy reduced buffer contents back to beginning
				// of CRDT log file, effectively deleting the first line.
				newNumOfBytes, err := io.Copy(sender.updLog, buf)
				if err != nil {
					log.Fatalf("[comm.HandleMessages] Error during copying buffer contents back to CRDT log file: %s\n", err.Error())
				}

				// Now, truncate log file size to exact amount
				// of bytes copied from buffer.
				err = sender.updLog.Truncate(newNumOfBytes)
				if err != nil {
					log.Fatalf("[comm.HandleMessages] Could not truncate CRDT log file: %s\n", err.Error())
				}

				// Sync changes to stable storage.
				err = sender.updLog.Sync()
				if err != nil {
					log.Fatalf("[comm.HandleMessages] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
				}

				// Reset position to beginning of file.
				_, err = sender.updLog.Seek(0, os.SEEK_SET)
				if err != nil {
					log.Fatalf("[comm.HandleMessages] Could not reset position in CRDT log file: %s\n", err.Error())
				}
			}

		// Wait for an incoming message to send.
		case payload := <-sender.inc:

			log.Println("Message to send")

			// Write it to message log file.
			_, err := sender.writeLog.WriteString(payload)
			if err != nil {
				log.Fatalf("[comm.HandleMessages] Writing to CRDT log file failed with: %s\n", err.Error())
			}

			// Save to stable storage.
			err = sender.writeLog.Sync()
			if err != nil {
				log.Fatalf("[comm.HandleMessages] Syncing CRDT log file to stable storage failed with: %s\n", err.Error())
			}

			log.Printf("[comm.HandleMessages] Wrote to CRDT log file: '%s'\n", payload)

			// Inidicate consecutive loop iterations
			// that a message is waiting in log.
			msgInLog <- true
		}
	}
}
