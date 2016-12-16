package comm

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"crypto/tls"
)

// Structs

// Sender bundles information needed for sending
// out sync messages via CRDTs.
type Sender struct {
	lock          *sync.Mutex
	name          string
	tlsConfig     *tls.Config
	intlConnRetry int
	inc           chan string
	msgInLog      chan struct{}
	writeLog      *os.File
	updLog        *os.File
	incVClock     chan string
	updVClock     chan map[string]int
	nodes         map[string]*tls.Conn
	wg            *sync.WaitGroup
	shutdown      chan struct{}
}

// Functions

// InitSender initializes above struct and sets
// default values for most involved elements to start
// with. It returns a channel local processes can put
// CRDT changes into, so that those changes will be
// communicated to connected nodes.
func InitSender(name string, logFilePath string, tlsConfig *tls.Config, retry int, incVClock chan string, updVClock chan map[string]int, downSender chan struct{}, nodes map[string]*tls.Conn) (chan string, error) {

	// Create and initialize what we need for
	// a CRDT sender routine.
	sender := &Sender{
		lock:          new(sync.Mutex),
		name:          name,
		tlsConfig:     tlsConfig,
		intlConnRetry: retry,
		inc:           make(chan string),
		msgInLog:      make(chan struct{}, 1),
		incVClock:     incVClock,
		updVClock:     updVClock,
		nodes:         nodes,
		wg:            new(sync.WaitGroup),
		shutdown:      make(chan struct{}, 2),
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

			// End infinite loop.
			break

		default:

			// Receive CRDT payload to send to other nodes
			// on incoming channel.
			payload, ok := <-sender.inc

			if ok {

				// If payload does not end with a newline symbol,
				// append one to it.
				if strings.HasSuffix(payload, "\n") != true {
					payload = fmt.Sprintf("%s\n", payload)
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

	sender.wg.Done()
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

			// End infinite loop.
			break

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

				for i, conn := range sender.nodes {

					var err error

					// Test long-lived connection.
					_, err = conn.Write([]byte("> ping <\n"))
					if err != nil {
						log.Fatalf("[comm.SendMsgs] Sending ping to node '%s' failed with: %s\n", i, err.Error())
					}

					// Write message to TLS connections.
					_, err = fmt.Fprintf(conn, "%s\n", marshalledMsg)
					for err != nil {

						log.Printf("[comm.SendMsgs] Sending to node '%s' failed, trying to recover...\n", i)

						// Define an error we can deal with.
						okError := fmt.Sprintf("write tcp %s->%s: write: broken pipe", conn.LocalAddr(), conn.RemoteAddr())

						// Extract address to reconnect to.
						addrParts := strings.Split(conn.RemoteAddr().String(), ":")

						if err.Error() == okError {

							// Connection was lost. Reconnect.
							conn, err = ReliableConnect(sender.name, i, addrParts[0], addrParts[1], sender.tlsConfig, 0, sender.intlConnRetry)
							if err != nil {
								log.Fatalf("[comm.SendMsgs] Could not reestablish connection with '%s': %s\n", i, err.Error())
							}

							// Replace old connection with new.
							sender.nodes[i] = conn
						} else {
							log.Fatalf("[comm.SendMsgs] Could not reestablish connection with '%s': %s\n", i, err.Error())
						}

						// Wait configured time before attempting next transfer.
						time.Sleep(time.Duration(sender.intlConnRetry) * time.Millisecond)

						// Retry transfer.
						_, err = fmt.Fprintf(conn, "%s\n", marshalledMsg)
					}
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

	sender.wg.Done()
}
