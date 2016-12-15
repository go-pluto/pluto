package imap

import (
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
			Contexts:         make(map[string]*Context),
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

		// Try to connect to sync port of each worker node this storage
		// node is serving as long-term storage backend as.
		c, err := comm.ReliableConnect("storage", workerName, workerNode.IP, workerNode.SyncPort, internalTLSConfig, config.IntlConnWait, config.IntlConnRetry)
		if err != nil {
			return nil, err
		}

		// Save connection for later use.
		curCRDTSubnet := make(map[string]*tls.Conn)
		curCRDTSubnet[workerName] = c
		storage.Connections[workerName] = c

		// Init sending part of CRDT communication and send messages in background.
		storage.SyncSendChans[workerName], err = comm.InitSender("storage", sendCRDTLog, internalTLSConfig, config.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, curCRDTSubnet)
		if err != nil {
			return nil, err
		}

		// Apply received CRDT messages in background.
		go storage.ApplyCRDTUpd(applyCRDTUpdChan, doneCRDTUpdChan)
	}

	// Start to listen for incoming internal connections on defined IP and mail port.
	storage.MailSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.MailPort), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitStorage] Listening for internal IMAP TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitStorage] Listening for incoming IMAP requests on %s.\n", storage.MailSocket.Addr())

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

	// Create a new connection struct for incoming request.
	c := NewConnection(conn)

	// Receive opening information.
	opening, err := c.Receive()
	if err != nil {
		c.ErrorLogOnly("Encountered receive error", err)
		return
	}

	// As long as this node did not receive an indicator that
	// the system is about to shut down, we accept requests.
	for opening != "> done <" {

		// Extract the prefixed clientID and update or create context.
		clientID, origWorker, err := storage.UpdateClientContext(opening)
		if err != nil {
			c.ErrorLogOnly("Error extracting context", err)
			return
		}

		// Receive incoming actual client command.
		rawReq, err := c.Receive()
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			// In case of failure, wait for next sent command.
			rawReq, err = c.Receive()
			if err != nil {
				c.ErrorLogOnly("Encountered receive error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		// Read-lock storage shortly.
		storage.lock.RLock()

		// Retrieve sync channel for node.
		workerSyncChan := storage.SyncSendChans[origWorker]

		// Release read-lock on storage.
		storage.lock.RUnlock()

		switch {

		case rawReq == "> done <":
			// Remove context of connection for this client
			// from structure that keeps track of it.
			// Effectively destroying all authentication and
			// IMAP state information about this client.
			delete(storage.Contexts, clientID)

		case req.Command == "SELECT":
			if ok := storage.Select(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "CREATE":
			if ok := storage.Create(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "DELETE":
			if ok := storage.Delete(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "LIST":
			if ok := storage.List(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "APPEND":
			if ok := storage.Append(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "EXPUNGE":
			if ok := storage.Expunge(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		case req.Command == "STORE":
			if ok := storage.Store(c, req, clientID, workerSyncChan); ok {

				// If successful, signal end of operation to proxy node.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
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
		opening, err = c.Receive()
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}
	}
}
