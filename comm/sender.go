package comm

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"crypto/tls"
)

// Structs

// Sender bundles information needed for sending
// out sync messages via CRDTs.
type Sender struct {
	lock            *sync.Mutex
	name            string
	tlsConfig       *tls.Config
	intlConnTimeout int
	intlConnRetry   int
	inc             chan string
	msgInLog        chan struct{}
	writeLog        *os.File
	updLog          *os.File
	incVClock       chan string
	updVClock       chan map[string]int
	nodes           map[string]string
	wg              *sync.WaitGroup
	shutdown        chan struct{}
}

// Functions

// InitSender initializes above struct and sets
// default values for most involved elements to start
// with. It returns a channel local processes can put
// CRDT changes into, so that those changes will be
// communicated to connected nodes.
func InitSender(name string, logFilePath string, tlsConfig *tls.Config, timeout int, retry int, incVClock chan string, updVClock chan map[string]int, downSender chan struct{}, nodes map[string]string) (chan string, error) {

	// Create and initialize what we need for
	// a CRDT sender routine.
	sender := &Sender{
		lock:            new(sync.Mutex),
		name:            name,
		tlsConfig:       tlsConfig,
		intlConnTimeout: timeout,
		intlConnRetry:   retry,
		inc:             make(chan string),
		msgInLog:        make(chan struct{}, 1),
		incVClock:       incVClock,
		updVClock:       updVClock,
		nodes:           nodes,
		wg:              new(sync.WaitGroup),
		shutdown:        make(chan struct{}, 2),
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

	// Start eventual shutdown routine in background.
	go sender.Shutdown(downSender)

	// Start brokering routine in background.
	sender.wg.Add(1)
	go sender.BrokerMsgs()

	// Start sending routine in background.
	sender.wg.Add(1)
	go sender.SendMsgs()

	// If we just started the application, perform an
	// initial run to check if log file contains elements.
	sender.msgInLog <- struct{}{}

	// Return this channel to pass to processes.
	return sender.inc, nil
}

// Shutdown awaits a sender global shutdown signal and
// in turn instructs involved goroutines to finish and
// clean up open files.
func (sender *Sender) Shutdown(downSender chan struct{}) {

	// Wait for signal.
	<-downSender

	log.Printf("[comm.Shutdown] sender: shutting down...\n")

	// Instruct brokering and sending routine to shut down
	// and clean up their respective file descriptors.
	sender.shutdown <- struct{}{}
	sender.shutdown <- struct{}{}

	// Close involved channels.
	close(sender.inc)
	close(sender.msgInLog)

	// Wait for both to indicate finish.
	sender.wg.Wait()

	log.Printf("[comm.Shutdown] sender: done!\n")
}

// BrokerMsgs awaits a CRDT message to send to downstream
// replicas from one of the local processes on channel inc.
// It stores the message for sending in a dedicated CRDT log
// file and passes on a signal that a new message is available.
func (sender *Sender) BrokerMsgs() {

	for {

		select {

		// Check if a shutdown signal was sent.
		case <-sender.shutdown:

			// If so, close file handler.
			sender.lock.Lock()
			sender.writeLog.Close()
			sender.lock.Unlock()

			// Call done handler of wait group for this
			// routine on exiting this function.
			defer sender.wg.Done()
			return

		default:

			// Receive CRDT payload to send to other nodes
			// on incoming channel.
			payload, ok := <-sender.inc

			if ok {

				// If payload does not end with a newline symbol,
				// append one to it.
				if strings.HasSuffix(payload, "\r\n") != true {
					payload = fmt.Sprintf("%s\r\n", payload)
				}

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

				// Inidicate consecutive loop iterations
				// that a message is waiting in log.
				if len(sender.msgInLog) < 1 {
					sender.msgInLog <- struct{}{}
				}
			}
		}
	}
}

// SendMsgs waits for a signal indicating that a message
// is waiting in the log file to be send out and sends that
// to all downstream nodes.
func (sender *Sender) SendMsgs() {

	for {

		select {

		// Check if a shutdown signal was sent.
		case <-sender.shutdown:

			// If so, close file handler.
			sender.lock.Lock()
			sender.updLog.Close()
			sender.lock.Unlock()

			// Call done handler of wait group for this
			// routine on exiting this function.
			defer sender.wg.Done()
			return

		default:

			// Wait for signal that new message was written to
			// log file so that we can send it out.
			_, ok := <-sender.msgInLog

			if ok {

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

				// Create a new message for message values.
				msg := InitMessage()

				// Set this node's name as sending part.
				msg.Sender = sender.name

				// Send this node's name on incVClock channel to
				// request an increment of its vector clock value.
				sender.incVClock <- sender.name

				// Wait for updated vector clock to be sent back
				// on other defined channel.
				msg.VClock = <-sender.updVClock

				// Remove trailing newline symbol from payload.
				msg.Payload = strings.TrimSpace(payload)

				// Marshall message.
				marshalledMsg := msg.String()

				// Unlock mutex.
				sender.lock.Unlock()

				for nodeName, nodeAddr := range sender.nodes {

					// Connect to node.
					conn, err := ReliableConnect(nodeName, nodeAddr, sender.tlsConfig, sender.intlConnRetry)
					if err != nil {
						log.Fatalf("[comm.SendMsgs] Failed to connect to %s: %s\n", err.Error())
					}

					// Send payload reliably to other nodes.
					err = ReliableSend(conn, marshalledMsg, nodeName, nodeAddr, sender.tlsConfig, sender.intlConnTimeout, sender.intlConnRetry)
					if err != nil {
						log.Fatalf("[comm.SendMsgs] Failed to send: %s\n", err.Error())
					}
				}

				// Lock mutex.
				sender.lock.Lock()

				// Retrieve file information.
				info, err = sender.updLog.Stat()
				if err != nil {
					log.Fatalf("[comm.SendMsgs] Could not get CRDT log file information: %s\n", err.Error())
				}

				// Create a buffer of capacity of read file size.
				buf = bytes.NewBuffer(make([]byte, 0, info.Size()))

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
				_, err = buf.ReadString('\n')
				if (err != nil) && (err != io.EOF) {
					log.Fatalf("[comm.SendMsgs] Error during extraction of first line in CRDT log file: %s\n", err.Error())
				}

				// Reset position to beginning of file.
				_, err = sender.updLog.Seek(0, os.SEEK_SET)
				if err != nil {
					log.Fatalf("[comm.SendMsgs] Could not reset position in CRDT log file: %s\n", err.Error())
				}

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
				if len(sender.msgInLog) < 1 {
					sender.msgInLog <- struct{}{}
				}
			}
		}
	}
}
