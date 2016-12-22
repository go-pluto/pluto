package imap

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/crypto"
)

// Worker struct bundles information needed in
// operation of a worker node.
type Worker struct {
	*IMAPNode
	Name         string
	SyncSendChan chan string
}

// FailoverWorker represents a reduced IMAPNode
// that simply writes through traffic to storage node.
type FailoverWorker struct {
	lock          *sync.RWMutex
	Name          string
	MailSocket    net.Listener
	IntlTLSConfig *tls.Config
	Connections   map[string]*tls.Conn
	Config        *config.Config
	ShutdownChan  chan struct{}
}

// Functions

// InitWorker listens for TLS connections on a TCP socket
// opened up on supplied IP address and port as well as connects
// to involved storage node. It returns those information bundeled
// in above Worker struct.
func InitWorker(config *config.Config, workerName string) (*Worker, error) {

	var err error

	// Initialize and set fields.
	worker := &Worker{
		IMAPNode: &IMAPNode{
			lock:             new(sync.RWMutex),
			Connections:      make(map[string]*tls.Conn),
			Contexts:         make(map[string]*Context),
			MailboxStructure: make(map[string]map[string]*crdt.ORSet),
			MailboxContents:  make(map[string]map[string][]string),
			Config:           config,
		},
		Name:         workerName,
		SyncSendChan: make(chan string),
	}

	// Check if supplied worker with workerName actually is configured.
	if _, ok := config.Workers[worker.Name]; !ok {

		var workerID string

		// Retrieve first valid worker ID to provide feedback.
		for workerID = range config.Workers {
			break
		}

		return nil, fmt.Errorf("[imap.InitWorker] Specified worker ID does not exist in config file. Please provide a valid one, for example '%s'.\n", workerID)
	}

	// We checked for name existence, now set correct paths.
	worker.CRDTLayerRoot = config.Workers[workerName].CRDTLayerRoot
	worker.MaildirRoot = config.Workers[workerName].MaildirRoot

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(worker.CRDTLayerRoot, "*"))
	if err != nil {
		return nil, fmt.Errorf("[imap.InitWorker] Globbing for CRDT folders of users failed with: %s\n", err.Error())
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitWorker] Error during stat'ing possible user CRDT folder: %s\n", err.Error())
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return nil, fmt.Errorf("[imap.InitWorker] Reading CRDT failed: %s\n", err.Error())
			}

			// Store main CRDT in designated map for user name.
			worker.MailboxStructure[userName] = make(map[string]*crdt.ORSet)
			worker.MailboxStructure[userName]["Structure"] = userMainCRDT

			// Already initialize slice to track order in mailbox.
			worker.MailboxContents[userName] = make(map[string][]string)

			// Retrieve all mailboxes the user possesses
			// according to main CRDT.
			userMailboxes := userMainCRDT.GetAllValues()

			for _, userMailbox := range userMailboxes {

				// Read in each mailbox CRDT from file.
				userMailboxCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, fmt.Sprintf("%s.log", userMailbox)))
				if err != nil {
					return nil, fmt.Errorf("[imap.InitWorker] Reading CRDT failed: %s\n", err.Error())
				}

				// Store each read-in CRDT in map under the respective
				// mailbox name in user's main CRDT.
				worker.MailboxStructure[userName][userMailbox] = userMailboxCRDT

				// Read in mails in respective mailbox in order to
				// allow sequence numbers actions.
				worker.MailboxContents[userName][userMailbox] = userMailboxCRDT.GetAllValues()
			}
		}
	}

	// Load internal TLS config.
	internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Workers[worker.Name].TLS.CertLoc, config.Workers[worker.Name].TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming internal connections on defined IP and sync port.
	worker.SyncSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[worker.Name].IP, config.Workers[worker.Name].SyncPort), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitWorker] Listening for internal sync TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitWorker] Listening for incoming sync requests on %s.\n", worker.SyncSocket.Addr())

	// Start to listen for incoming internal connections on defined IP and mail port.
	worker.MailSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[worker.Name].IP, config.Workers[worker.Name].MailPort), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitWorker] Listening for internal IMAP TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitWorker] Listening for incoming IMAP requests on %s.\n", worker.MailSocket.Addr())

	// Initialize channels for this node.
	applyCRDTUpdChan := make(chan string)
	doneCRDTUpdChan := make(chan struct{})
	downRecv := make(chan struct{})
	downSender := make(chan struct{})

	// Construct path to receiving and sending CRDT logs for storage node.
	recvCRDTLog := filepath.Join(worker.CRDTLayerRoot, "receiving.log")
	sendCRDTLog := filepath.Join(worker.CRDTLayerRoot, "sending.log")
	vclockLog := filepath.Join(worker.CRDTLayerRoot, "vclock.log")

	// Initialize receiving goroutine for sync operations.
	chanIncVClockWorker, chanUpdVClockWorker, err := comm.InitReceiver(worker.Name, recvCRDTLog, vclockLog, worker.SyncSocket, applyCRDTUpdChan, doneCRDTUpdChan, downRecv, []string{"storage"})
	if err != nil {
		return nil, err
	}

	// Try to connect to sync port of storage node to which this node
	// sends data for long-term storage, but in background.
	c, err := comm.ReliableConnect(worker.Name, "storage", config.Storage.IP, config.Storage.SyncPort, internalTLSConfig, config.IntlConnRetry)
	if err != nil {
		return nil, err
	}

	// Save connection for later use.
	worker.Connections["storage"] = c

	// Init sending part of CRDT communication and send messages in background.
	worker.SyncSendChan, err = comm.InitSender(worker.Name, sendCRDTLog, internalTLSConfig, config.IntlConnTimeout, config.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, worker.Connections)
	if err != nil {
		return nil, err
	}

	// Apply received CRDT messages in background.
	go worker.ApplyCRDTUpd(applyCRDTUpdChan, doneCRDTUpdChan)

	return worker, nil
}

// InitFailoverWorker initializes a worker node that acts as a
// passthrough-failover of the worker node specified via workerName.
// This results in a "dumb" proxy node that forwards all received
// traffic from distributor directly to storage node.
func InitFailoverWorker(config *config.Config, workerName string) (*FailoverWorker, error) {

	var err error

	// Initialize and set fields.
	failWorker := &FailoverWorker{
		lock:        new(sync.RWMutex),
		Name:        workerName,
		Connections: make(map[string]*tls.Conn),
		Config:      config,
	}

	// Check if supplied worker with workerName actually is configured.
	if _, ok := config.Workers[failWorker.Name]; !ok {

		var workerID string

		// Retrieve first valid worker ID to provide feedback.
		for workerID = range config.Workers {
			break
		}

		return nil, fmt.Errorf("[imap.InitFailoverWorker] Specified worker ID does not exist in config file. Please provide a valid one, for example '%s'.\n", workerID)
	}

	// Load internal TLS config.
	failWorker.IntlTLSConfig, err = crypto.NewInternalTLSConfig(config.Workers[failWorker.Name].TLS.CertLoc, config.Workers[failWorker.Name].TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming internal connections on defined IP and mail port.
	failWorker.MailSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[failWorker.Name].IP, config.Workers[failWorker.Name].MailPort), failWorker.IntlTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitFailoverWorker] Listening for internal IMAP TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitFailoverWorker] Listening for incoming IMAP requests on %s.\n", failWorker.MailSocket.Addr())

	// Try to connect to mail port of storage node to which this node
	// forwards all traffic it received from distributor.
	c, err := comm.ReliableConnect(failWorker.Name, "storage", config.Storage.IP, config.Storage.MailPort, failWorker.IntlTLSConfig, config.IntlConnRetry)
	if err != nil {
		return nil, err
	}

	// Save connection for later use.
	failWorker.Connections["storage"] = c

	return failWorker, nil
}

// Run loops over incoming requests at worker and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (worker *Worker) Run() error {

	for {

		// Accept request or fail on error.
		conn, err := worker.MailSocket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at %s failed with: %s\n", worker.Name, err.Error())
		}

		// Dispatch into own goroutine.
		go worker.HandleConnection(conn)
	}
}

// HandleConnection is the main worker routine where all
// incoming requests against worker nodes have to go through.
func (worker *Worker) HandleConnection(conn net.Conn) {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		log.Printf("[imap.HandleConnection] Worker %s could not convert connection into TLS connection.\n", worker.Name)
		return
	}

	// Create a new connection struct for incoming request.
	c := NewConnection(tlsConn)

	// Receive opening information.
	opening, err := comm.InternalReceive(c.Reader)
	if err != nil {
		c.ErrorLogOnly("Encountered receive error", err)
		return
	}

	// As long as the distributor node did not indicate that
	// the system is about to shut down, we accept requests.
	for opening != "> done <" {

		// Extract the prefixed clientID and update or create context.
		clientID, err := worker.UpdateClientContext(opening)
		if err != nil {
			c.ErrorLogOnly("Error extracting context", err)
			return
		}

		// Receive incoming actual client command.
		rawReq, err := comm.InternalReceive(c.Reader)
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := comm.InternalSend(c.Conn, err.Error(), worker.Name, "distributor")
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			// In case of failure, wait for next sent command.
			rawReq, err = comm.InternalReceive(c.Reader)
			if err != nil {
				c.ErrorLogOnly("Encountered receive error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		switch {

		case rawReq == "> done <":
			// Remove context of connection for this client
			// from structure that keeps track of it.
			// Effectively destroying all authentication and
			// IMAP state information about this client.
			delete(worker.Contexts, clientID)

		case req.Command == "SELECT":
			if ok := worker.Select(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "CREATE":
			if ok := worker.Create(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "DELETE":
			if ok := worker.Delete(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "LIST":
			if ok := worker.List(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "APPEND":
			if ok := worker.Append(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "EXPUNGE":
			if ok := worker.Expunge(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "STORE":
			if ok := worker.Store(c, req, clientID, worker.SyncSendChan); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := comm.InternalSend(c.Conn, fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag), worker.Name, "distributor")
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			err = c.SignalSessionDone(nil)
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}
		}

		// Receive next incoming proxied request.
		opening, err = comm.InternalReceive(c.Reader)
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}
	}
}

// RunFailover is the main method called when starting a
// failover worker node. It accepts IMAP connections and
// dispatches them into own goroutines.
func (failWorker *FailoverWorker) RunFailover() error {

	for {

		// Accept request or fail on error.
		conn, err := failWorker.MailSocket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at failover %s failed with: %s\n", failWorker.Name, err.Error())
		}

		// Dispatch into own goroutine.
		go failWorker.HandleFailover(conn)
	}
}

// HandleFailover is the function new IMAP connections
// incoming at a failover worker node are dispatched into
// in. It takes care of message forwarding to storage and
// reply return to distributor.
func (failWorker *FailoverWorker) HandleFailover(conn net.Conn) {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		log.Printf("[imap.HandleFailover] Failover %s could not convert connection into TLS connection.\n", failWorker.Name)
		return
	}

	// Create a new connection struct for incoming request.
	c := NewConnection(tlsConn)

	// Receive opening information.
	opening, err := comm.InternalReceive(c.Reader)
	if err != nil {
		c.ErrorLogOnly("Encountered receive error", err)
		return
	}

	// As long as the distributor node did not indicate that
	// the system is about to shut down, we accept requests.
	for opening != "> done <" {

		// Extract the prefixed clientID.
		clientID, err := failWorker.ExtractClientContext(opening)
		if err != nil {
			c.ErrorLogOnly("Error extracting context", err)
			return
		}

		// Receive incoming actual client command.
		rawReq, err := comm.InternalReceive(c.Reader)
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}

		failWorker.lock.RLock()

		// Save connection information to storage for later use.
		storageConn := failWorker.Connections["storage"]
		storageIP := failWorker.Config.Storage.IP
		storagePort := failWorker.Config.Storage.MailPort

		failWorker.lock.RUnlock()

		// Inform storage about which session will continue.
		conn, err := c.SignalSessionPrefixStorage(clientID, storageConn, failWorker.Name, "storage", storageIP, storagePort, failWorker.IntlTLSConfig, failWorker.Config.IntlConnTimeout, failWorker.Config.IntlConnRetry)
		if err != nil {
			c.ErrorLogOnly("Encountered send error when failover worker signalled context to storage", err)
			return
		}

		failWorker.lock.Lock()

		// Replace stored connection by possibly new one.
		failWorker.Connections["storage"] = conn

		failWorker.lock.Unlock()

		// Create a buffered reader from storage connection.
		storageReader := bufio.NewReader(conn)

		// Send received client command to storage.
		err = comm.InternalSend(conn, rawReq, failWorker.Name, "storage")
		if err != nil {
			c.ErrorLogOnly("Encountered send error to storage", err)
			return
		}

		// Reserve space for answer buffer.
		bufResp := make([]string, 0, 6)

		// Receive incoming storage response.
		curResp, err := comm.InternalReceive(storageReader)
		if err != nil {
			c.ErrorLogOnly("Encountered receive error from storage", err)
			return
		}

		// As long as the storage node has not indicated
		// the end of the current operation, continue
		// to buffer answers.
		for (curResp != "> done <") && (curResp != "> error <") && (strings.HasPrefix(curResp, "> literal: ") != true) {

			// Append it to answer buffer.
			bufResp = append(bufResp, curResp)

			// Receive incoming storage response.
			curResp, err = comm.InternalReceive(storageReader)
			if err != nil {
				c.ErrorLogOnly("Encountered receive error from storage", err)
				return
			}
		}

		for i := range bufResp {

			// Send all buffered storage answers to distributor.
			err = comm.InternalSend(c.Conn, bufResp[i], failWorker.Name, "distributor")
			if err != nil {
				c.ErrorLogOnly("Encountered send error to distributor", err)
				return
			}
		}

		// Special case: We expect literal data in form of a
		// RFC defined mail message.
		if strings.HasPrefix(curResp, "> literal: ") {

			// Strip off left and right elements of signal.
			// This leaves the awaited amount of bytes.
			numBytesString := strings.TrimLeft(curResp, "> literal: ")
			numBytesString = strings.TrimRight(numBytesString, " <")

			// Convert string amount to int.
			numBytes, err := strconv.Atoi(numBytesString)
			if err != nil {
				c.ErrorLogOnly("Encountered conversion error for string to int", err)
				return
			}

			// Signal distributor what amount of literal bytes we are expecting.
			err = c.SignalAwaitingLiteral(numBytes)
			if err != nil {
				c.ErrorLogOnly("Error during signalling distributor literal byte number", err)
				return
			}

			// Reserve space for exact amount of expected data.
			msgBuffer := make([]byte, numBytes)

			// Read in that amount from connection to client.
			_, err = io.ReadFull(c.Reader, msgBuffer)
			if err != nil {
				c.ErrorLogOnly("Encountered error while reading client literal data", err)
				return
			}

			// Pass on data to storage. Mails have to be ended by
			// newline symbol.
			_, err = fmt.Fprintf(conn, "%s", msgBuffer)
			if err != nil {
				c.ErrorLogOnly("Encountered passing send error to storage", err)
				return
			}

			// Reserve space for answer buffer.
			bufResp := make([]string, 0, 6)

			// Receive incoming storage response.
			curResp, err := comm.InternalReceive(storageReader)
			if err != nil {
				c.ErrorLogOnly("Encountered receive error from storage after literal data was sent", err)
				return
			}

			// As long as the storage node has not indicated
			// the end of the current operation, continue
			// to buffer answers.
			for (curResp != "> done <") && (curResp != "> error <") {

				// Append it to answer buffer.
				bufResp = append(bufResp, curResp)

				// Receive incoming storage response.
				curResp, err = comm.InternalReceive(storageReader)
				if err != nil {
					c.ErrorLogOnly("Encountered receive error from storage after literal data was sent", err)
					return
				}
			}

			for i := range bufResp {

				// Send all buffered storage answers to distributor.
				err = comm.InternalSend(c.Conn, bufResp[i], failWorker.Name, "distributor")
				if err != nil {
					c.ErrorLogOnly("Encountered send error to distributor after literal data was sent", err)
					return
				}
			}
		}

		if (curResp == "> done <") || (strings.HasPrefix(curResp, "> literal: ")) {

			// If successful, signal end of operation to distributor.
			err := c.SignalSessionDone(nil)
			if err != nil {
				c.ErrorLogOnly("Encountered send error signalling done to distributor while in proxy", err)
				return
			}
		} else if curResp == "> error <" {

			// If the involved storage node indicated that an error
			// occurred, terminate connection to client.
			c.ErrorLogOnly("Storage indicated error, failover worker passed on", err)
			return
		}

		// Receive next incoming proxied request.
		opening, err = comm.InternalReceive(c.Reader)
		if err != nil {
			c.ErrorLogOnly("Encountered receive error on failover waiting for next request", err)
			return
		}
	}
}
