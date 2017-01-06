package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/crypto"
)

// Structs

// Storage struct bundles information needed in
// operation of a storage node.
type Storage struct {
	*IMAPNode
	SyncSendChans map[string]chan string
}

// Functions

// InitStorage listens for TLS connections on a TCP socket
// opened up on supplied IP address. It returns those
// information bundeled in above Storage struct.
func InitStorage(config *config.Config) (*Storage, error) {

	// Initialize and set fields.
	storage := &Storage{
		IMAPNode: &IMAPNode{
			lock:             new(sync.RWMutex),
			Connections:      make(map[string]*tls.Conn),
			MailboxStructure: make(map[string]map[string]*crdt.ORSet),
			MailboxContents:  make(map[string]map[string][]string),
			CRDTLayerRoot:    config.Storage.CRDTLayerRoot,
			MaildirRoot:      config.Storage.MaildirRoot,
			Config:           config,
		},
		SyncSendChans: make(map[string]chan string),
	}

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(storage.CRDTLayerRoot, "*"))
	if err != nil {
		return nil, fmt.Errorf("[imap.InitStorage] Globbing for CRDT folders of users failed with: %s\n", err.Error())
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitStorage] Error during stat'ing possible user CRDT folder: %s\n", err.Error())
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return nil, fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %s\n", err.Error())
			}

			// Store main CRDT in designated map for user name.
			storage.MailboxStructure[userName] = make(map[string]*crdt.ORSet)
			storage.MailboxStructure[userName]["Structure"] = userMainCRDT

			// Already initialize slice to track order in mailbox.
			storage.MailboxContents[userName] = make(map[string][]string)

			// Retrieve all mailboxes the user possesses
			// according to main CRDT.
			userMailboxes := userMainCRDT.GetAllValues()

			for _, userMailbox := range userMailboxes {

				// Read in each mailbox CRDT from file.
				userMailboxCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, fmt.Sprintf("%s.log", userMailbox)))
				if err != nil {
					return nil, fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %s\n", err.Error())
				}

				// Store each read-in CRDT in map under the respective
				// mailbox name in user's main CRDT.
				storage.MailboxStructure[userName][userMailbox] = userMailboxCRDT

				// Read in mails in respective mailbox in order to
				// allow sequence numbers actions.
				storage.MailboxContents[userName][userMailbox] = userMailboxCRDT.GetAllValues()
			}
		}
	}

	// Load internal TLS config.
	internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Storage.TLS.CertLoc, config.Storage.TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming internal connections on defined IP and sync port.
	storage.SyncSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.SyncPort), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitStorage] Listening for internal sync TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitStorage] Listening for incoming sync requests on %s.\n", storage.SyncSocket.Addr())

	// Start to listen for incoming internal connections on defined IP and mail port.
	storage.MailSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.MailPort), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitStorage] Listening for internal IMAP TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitStorage] Listening for incoming IMAP requests on %s.\n", storage.MailSocket.Addr())

	for workerName, workerNode := range config.Workers {

		// Initialize channels for this node.
		applyCRDTUpdChan := make(chan string)
		doneCRDTUpdChan := make(chan struct{})
		downRecv := make(chan struct{})
		downSender := make(chan struct{})

		// Construct path to receiving and sending CRDT logs for
		// current worker node.
		recvCRDTLog := filepath.Join(storage.CRDTLayerRoot, fmt.Sprintf("receiving-%s.log", workerName))
		sendCRDTLog := filepath.Join(storage.CRDTLayerRoot, fmt.Sprintf("sending-%s.log", workerName))
		vclockLog := filepath.Join(storage.CRDTLayerRoot, fmt.Sprintf("vclock-%s.log", workerName))

		// Initialize a receiving goroutine for sync operations
		// for each worker node.
		chanIncVClockWorker, chanUpdVClockWorker, err := comm.InitReceiver("storage", recvCRDTLog, vclockLog, storage.SyncSocket, applyCRDTUpdChan, doneCRDTUpdChan, downRecv, []string{workerName})
		if err != nil {
			return nil, err
		}

		// Create subnet to distribute CRDT changes in.
		curCRDTSubnet := make(map[string]string)
		curCRDTSubnet[workerName] = fmt.Sprintf("%s:%s", workerNode.IP, workerNode.SyncPort)

		// Init sending part of CRDT communication and send messages in background.
		storage.SyncSendChans[workerName], err = comm.InitSender("storage", sendCRDTLog, internalTLSConfig, config.IntlConnTimeout, config.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, curCRDTSubnet)
		if err != nil {
			return nil, err
		}

		// Apply received CRDT messages in background.
		go storage.ApplyCRDTUpd(applyCRDTUpdChan, doneCRDTUpdChan)
	}

	return storage, nil
}

// Run loops over incoming requests at storage and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (storage *Storage) Run() error {

	for {

		// Accept request or fail on error.
		conn, err := storage.MailSocket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at storage failed with: %s\n", err.Error())
		}

		// Dispatch into own goroutine.
		go storage.HandleConnection(conn)
	}
}

// HandleConnection is the main storage routine where all
// incoming requests against this storage node have to go through.
func (storage *Storage) HandleConnection(conn net.Conn) {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		log.Printf("[imap.HandleConnection] Storage could not convert connection into TLS connection.\n")
		return
	}

	// Create a new connection struct for incoming request.
	c := &IMAPConnection{
		Connection: &Connection{
			IncConn:   tlsConn,
			IncReader: bufio.NewReader(tlsConn),
		},
		IMAPState: AUTHENTICATED,
	}

	// Receive opening information.
	clientInfo, err := c.InternalReceive(true)
	if err != nil {
		c.Error("Receive error waiting for client information", err)
		return
	}

	// Based on received client information, update IMAP
	// connection to reflect these information.
	origWorker, err := c.UpdateClientContext(clientInfo, storage.Config.Storage.CRDTLayerRoot, storage.Config.Storage.MaildirRoot)
	if err != nil {
		c.Error("Error extracting client information", err)
		return
	}

	// Receive actual client command.
	rawReq, err := c.InternalReceive(true)
	if err != nil {
		c.Error("Encountered receive error waiting for first request", err)
		return
	}

	// As long as the proxying node did not indicate that
	// the client connection was ended, we accept requests.
	for rawReq != "> done <" {

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.InternalSend(true, err.Error())
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			// In case of failure, wait for next sent command.
			rawReq, err = c.InternalReceive(true)
			if err != nil {
				c.Error("Encountered receive error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		storage.lock.RLock()

		// Retrieve sync channel for node.
		workerSyncChan := storage.SyncSendChans[origWorker]

		storage.lock.RUnlock()

		log.Printf("[imap.HandleConnection] Storage: working on failover request from %s: '%s'\n", origWorker, rawReq)

		switch {

		case req.Command == "SELECT":
			if ok := storage.Select(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case req.Command == "CREATE":
			if ok := storage.Create(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case req.Command == "DELETE":
			if ok := storage.Delete(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case req.Command == "LIST":
			if ok := storage.List(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case req.Command == "APPEND":
			if ok := storage.Append(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case req.Command == "EXPUNGE":
			if ok := storage.Expunge(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case req.Command == "STORE":
			if ok := storage.Store(c, req, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(true)
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.InternalSend(true, fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			err = c.SignalSessionDone(true)
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}

		// Receive next incoming proxied request.
		rawReq, err = c.InternalReceive(true)
		if err != nil {
			c.Error("Encountered receive error", err)
			return
		}
	}

	// Terminate connection after logout.
	err = c.Terminate()
	if err != nil {
		log.Fatalf("[imap.HandleConnection] Failed to terminate connection: %s\n", err.Error())
	}

	// Set IMAP state to logged out.
	c.IMAPState = LOGOUT
}
