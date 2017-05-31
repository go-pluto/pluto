package storage

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/imap"
)

// InternalConnection knows how to create internal tls connections
type InternalConnection interface {
	ReliableConnect(addr string) (*tls.Conn, error)
}

// Service defines the interface a storage node
// in a pluto network provides.
type Service interface {

	// Run loops over incoming requests at storage and
	// dispatches each one to a goroutine taking care of
	// the commands supplied.
	Run() error

	// HandleConnection is the main storage routine where all
	// incoming requests against this storage node have to go through.
	HandleConnection(net.Conn) error

	// Select sets the current mailbox based on supplied payload to user-instructed value.
	Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// Create attempts to create a mailbox with name taken from payload of request.
	Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// Delete an existing mailbox with all included content.
	Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// List allows clients to learn about the mailboxes
	// available and also returns the hierarchy delimiter.
	List(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// Append puts supplied message into specified mailbox.
	Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// Expunge deletes messages permanently from currently
	// selected mailbox that have been flagged as Deleted
	// prior to calling this function.
	Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// Store takes in message sequence numbers and some set
	// of flags to change in those messages and changes the
	// attributes for these mails throughout the system.
	Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool
}

type service struct {
	imapNode           *imap.IMAPNode
	SyncSendChans      map[string]chan string
	internalConnection InternalConnection
	config             config.Storage
}

// NewService takes in all required parameters for spinning
// up a new storage node, runs initialization code, and returns
// a service struct for this node type wrapping all information.
func NewService(internalConnection InternalConnection, config config.Storage, workers map[string]config.Worker) Service {

	s := &service{
		imapNode: &imap.IMAPNode{
			//lock:             new(sync.RWMutex), // TODO: should work without
			Connections:      make(map[string]*tls.Conn),
			MailboxStructure: make(map[string]map[string]*crdt.ORSet),
			MailboxContents:  make(map[string]map[string][]string),
			CRDTLayerRoot:    config.CRDTLayerRoot,
			MaildirRoot:      config.MaildirRoot,
			//Config:           config,
		},
		SyncSendChans:      make(map[string]chan string),
		internalConnection: internalConnection,
		config:             config,
	}

	// TODO: Probably better to move initializing into its own method, so we can check errors.

	if err := s.findFiles(); err != nil {
		return nil
	}

	// TODO: Move this out and inject from the outside
	//// Load internal TLS config.
	//internalTLSConfig, err := crypto.NewInternalTLSConfig(config.TLS.CertLoc, config.TLS.KeyLoc, config.RootCertLoc)
	//if err != nil {
	//	return nil, err
	//}
	//
	//// Start to listen for incoming internal connections on defined IP and sync port.
	//storage.SyncSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.ListenIP, config.SyncPort), internalTLSConfig)
	//if err != nil {
	//	return nil, fmt.Errorf("[imap.InitStorage] Listening for internal sync TLS connections failed with: %v", err)
	//}
	//
	//stdlog.Printf("[imap.InitStorage] Listening for incoming sync requests on %s.\n", config.SyncSocket.Addr())
	//
	//// Start to listen for incoming internal connections on defined IP and mail port.
	//storage.MailSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.ListenIP, config.MailPort), internalTLSConfig)
	//if err != nil {
	//	return nil, fmt.Errorf("[imap.InitStorage] Listening for internal IMAP TLS connections failed with: %v", err)
	//}
	//
	//stdlog.Printf("[imap.InitStorage] Listening for incoming IMAP requests on %s.\n", storage.MailSocket.Addr())

	for workerName, workerNode := range workers {

		// Initialize channels for this node.
		applyCRDTUpdChan := make(chan string)
		doneCRDTUpdChan := make(chan struct{})
		//downRecv := make(chan struct{})
		//downSender := make(chan struct{})

		//// Construct path to receiving and sending CRDT logs for
		//// current worker node.
		//recvCRDTLog := filepath.Join(s.imapNode.CRDTLayerRoot, fmt.Sprintf("receiving-%s.log", workerName))
		//sendCRDTLog := filepath.Join(s.imapNode.CRDTLayerRoot, fmt.Sprintf("sending-%s.log", workerName))
		//vclockLog := filepath.Join(s.imapNode.CRDTLayerRoot, fmt.Sprintf("vclock-%s.log", workerName))

		//// Initialize a receiving goroutine for sync operations
		//// for each worker node.
		//chanIncVClockWorker, chanUpdVClockWorker, err := comm.InitReceiver("storage", recvCRDTLog, vclockLog, s.imapNode.SyncSocket, applyCRDTUpdChan, doneCRDTUpdChan, downRecv, []string{workerName})
		//if err != nil {
		//	return err
		//}

		// Create subnet to distribute CRDT changes in.
		curCRDTSubnet := make(map[string]string)
		curCRDTSubnet[workerName] = fmt.Sprintf("%s:%s", workerNode.PublicIP, workerNode.SyncPort)

		// TODO: Use injected connection
		//// Init sending part of CRDT communication and send messages in background.
		//s.SyncSendChans[workerName], err = comm.InitSender("storage", sendCRDTLog, internalTLSConfig, config.IntlConnTimeout, config.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, curCRDTSubnet)
		//if err != nil {
		//	return nil, err
		//}

		// Apply received CRDT messages in background.
		go s.imapNode.ApplyCRDTUpd(applyCRDTUpdChan, doneCRDTUpdChan)
	}

	return s
}

func (s *service) findFiles() error {

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(s.imapNode.CRDTLayerRoot, "*"))
	if err != nil {
		return fmt.Errorf("[imap.InitStorage] Globbing for CRDT folders of users failed with: %v", err)
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return fmt.Errorf("[imap.InitStorage] Error during stat'ing possible user CRDT folder: %v", err)
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %v", err)
			}

			// Store main CRDT in designated map for user name.
			s.imapNode.MailboxStructure[userName] = make(map[string]*crdt.ORSet)
			s.imapNode.MailboxStructure[userName]["Structure"] = userMainCRDT

			// Already initialize slice to track order in mailbox.
			s.imapNode.MailboxContents[userName] = make(map[string][]string)

			// Retrieve all mailboxes the user possesses
			// according to main CRDT.
			userMailboxes := userMainCRDT.GetAllValues()

			for _, userMailbox := range userMailboxes {

				// Read in each mailbox CRDT from file.
				userMailboxCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, fmt.Sprintf("%s.log", userMailbox)))
				if err != nil {
					return fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %v", err)
				}

				// Store each read-in CRDT in map under the respective
				// mailbox name in user's main CRDT.
				s.imapNode.MailboxStructure[userName][userMailbox] = userMailboxCRDT

				// Read in mails in respective mailbox in order to
				// allow sequence numbers actions.
				s.imapNode.MailboxContents[userName][userMailbox] = userMailboxCRDT.GetAllValues()
			}
		}
	}

	return nil
}

func (s *service) Run() error {

	for {

		// Accept request or fail on error.
		conn, err := s.imapNode.MailSocket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at storage failed with: %v", err)
		}

		// Dispatch into own goroutine.
		go s.HandleConnection(conn)
	}
}

func (s *service) HandleConnection(conn net.Conn) error {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		return fmt.Errorf("[imap.HandleConnection] Storage could not convert connection into TLS connection")
	}

	// Create a new connection struct for incoming request.
	c := &imap.IMAPConnection{
		Connection: &imap.Connection{
			IncConn:   tlsConn,
			IncReader: bufio.NewReader(tlsConn),
		},
		State: imap.Authenticated,
	}

	// Receive opening information.
	clientInfo, err := c.InternalReceive(true)
	if err != nil {
		c.Error("Receive error waiting for client information", err)
		return nil
	}

	// Based on received client information, update IMAP
	// connection to reflect these information.
	origWorker, err := c.UpdateClientContext(clientInfo, s.imapNode.CRDTLayerRoot, s.imapNode.MaildirRoot)
	if err != nil {
		c.Error("Error extracting client information", err)
		return nil
	}

	// Receive actual client command.
	rawReq, err := c.InternalReceive(true)
	if err != nil {
		c.Error("Encountered receive error waiting for first request", err)
		return nil
	}

	// As long as the proxying node did not indicate that
	// the client connection was ended, we accept requests.
	for rawReq != "> done <" {

		// Parse received next raw request into struct.
		req, err := imap.ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.InternalSend(true, err.Error(), false, "")
			if err != nil {
				c.Error("Encountered send error", err)
				return nil
			}

			// In case of failure, wait for next sent command.
			rawReq, err = c.InternalReceive(true)
			if err != nil {
				c.Error("Encountered receive error", err)
				return nil
			}

			// Go back to beginning of loop.
			continue
		}

		// TODO: Lock inside that package?!
		//storage.lock.RLock()

		// Retrieve sync channel for node.
		workerSyncChan := s.SyncSendChans[origWorker]

		// TODO: Lock inside that package?!
		//storage.lock.RUnlock()

		switch {

		case req.Command == "SELECT":
			s.Select(c, req, workerSyncChan)

		case req.Command == "CREATE":
			s.Create(c, req, workerSyncChan)

		case req.Command == "DELETE":
			s.Delete(c, req, workerSyncChan)

		case req.Command == "LIST":
			s.List(c, req, workerSyncChan)

		case req.Command == "APPEND":
			s.Append(c, req, workerSyncChan)

		case req.Command == "EXPUNGE":
			s.Expunge(c, req, workerSyncChan)

		case req.Command == "STORE":
			s.Store(c, req, workerSyncChan)

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.InternalSend(true, fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag), false, "")
			if err != nil {
				c.Error("Encountered send error", err)
				return nil
			}

			err = c.SignalSessionDone(true)
			if err != nil {
				c.Error("Encountered send error", err)
				return nil
			}
		}

		// Receive next incoming proxied request.
		rawReq, err = c.InternalReceive(true)
		if err != nil {
			c.Error("Encountered receive error", err)
			return nil
		}
	}

	// Terminate connection after logout.
	err = c.Terminate()
	if err != nil {
		return fmt.Errorf("[imap.HandleConnection] Failed to terminate connection: %v", err)
	}

	// Set IMAP state to logged out.
	c.State = imap.Logout

	return nil
}

func (s *service) Select(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.Select(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

func (s *service) Create(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.Create(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

func (s *service) Delete(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.Delete(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

func (s *service) List(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.List(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

func (s *service) Append(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.Append(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

func (s *service) Expunge(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.Expunge(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

func (s *service) Store(c *imap.IMAPConnection, req *imap.Request, workerSyncChan chan string) bool {

	ok := s.imapNode.Store(c, req, workerSyncChan)
	if ok {

		// If successful, signal end of operation to proxy node.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}
