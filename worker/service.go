package worker

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

// Service defines the interface a worker node
// in a pluto network provides.
type Service interface {

	// Run loops over incoming requests at worker and
	// dispatches each one to a goroutine taking care of
	// the commands supplied.
	Run() error

	// HandleConnection is the main worker routine where all
	// incoming requests against worker nodes have to go through.
	HandleConnection(conn net.Conn) error

	// Select sets the current mailbox based on supplied
	// payload to user-instructed value.
	Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool

	// Create attempts to create a mailbox with
	// name taken from payload of request.
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
	Name               string
	SyncSendChan       chan string
	internalConnection InternalConnection
	config             config.Worker
}

// NewService takes in all required parameters for spinning
// up a new worker node, runs initialization code, and returns
// a service struct for this node type wrapping all information.
func NewService(internalConnection InternalConnection, config config.Worker, name string) Service {

	s := &service{
		imapNode: &imap.IMAPNode{
			//lock:             new(sync.RWMutex), // TODO: should work without
			Connections:      make(map[string]*tls.Conn),
			MailboxStructure: make(map[string]map[string]*crdt.ORSet),
			MailboxContents:  make(map[string]map[string][]string),
		},
		Name:               name,
		SyncSendChan:       make(chan string),
		internalConnection: internalConnection,
		config:             config,
	}

	// TODO: Probably better to move initializing into its own method, so we can check errors.

	if err := s.findFiles(); err != nil {
		return nil
	}

	// Set correct paths.
	s.imapNode.CRDTLayerRoot = config.CRDTLayerRoot
	s.imapNode.MaildirRoot = config.MaildirRoot

	// TODO: Move this out and inject from the outside
	//// Load internal TLS config.
	//internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Workers[worker.Name].TLS.CertLoc, config.Workers[worker.Name].TLS.KeyLoc, config.RootCertLoc)
	//if err != nil {
	//	return nil, err
	//}
	//
	//// Start to listen for incoming internal connections on defined IP and sync port.
	//worker.SyncSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[worker.Name].ListenIP, config.Workers[worker.Name].SyncPort), internalTLSConfig)
	//if err != nil {
	//	return nil, fmt.Errorf("[imap.InitWorker] Listening for internal sync TLS connections failed with: %v", err)
	//}
	//
	//stdlog.Printf("[imap.InitWorker] Listening for incoming sync requests on %s.\n", worker.SyncSocket.Addr())
	//
	//// Start to listen for incoming internal connections on defined IP and mail port.
	//worker.MailSocket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[worker.Name].ListenIP, config.Workers[worker.Name].MailPort), internalTLSConfig)
	//if err != nil {
	//	return nil, fmt.Errorf("[imap.InitWorker] Listening for internal IMAP TLS connections failed with: %v", err)
	//}
	//
	//stdlog.Printf("[imap.InitWorker] Listening for incoming IMAP requests on %s.\n", worker.MailSocket.Addr())

	// Initialize channels for this node.
	applyCRDTUpdChan := make(chan string)
	doneCRDTUpdChan := make(chan struct{})
	//downRecv := make(chan struct{})
	//downSender := make(chan struct{})

	// TODO: Probably inject as dependency from the outside too
	//// Construct path to receiving and sending CRDT logs for storage node.
	//recvCRDTLog := filepath.Join(s.imapNode.CRDTLayerRoot, "receiving.log")
	//sendCRDTLog := filepath.Join(s.imapNode.CRDTLayerRoot, "sending.log")
	//vclockLog := filepath.Join(s.imapNode.CRDTLayerRoot, "vclock.log")
	//
	//// Initialize receiving goroutine for sync operations.
	// chanIncVClockWorker, chanUpdVClockWorker, err := comm.InitReceiver(worker.Name, recvCRDTLog, vclockLog, worker.SyncSocket, internalTLSConfig, applyCRDTUpdChan, doneCRDTUpdChan, downRecv, []string{"storage"})
	//if err != nil {
	//	return nil
	//}
	//
	//// Create subnet to distribute CRDT changes in.
	//curCRDTSubnet := make(map[string]string)
	//curCRDTSubnet["storage"] = fmt.Sprintf("%s:%s", config.Storage.PublicIP, config.Storage.SyncPort)
	//
	//// Init sending part of CRDT communication and send messages in background.
	//s.SyncSendChan, err = comm.InitSender(s.Name, sendCRDTLog, internalTLSConfig, config.IntlConnTimeout, config.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, curCRDTSubnet)
	//if err != nil {
	//	return nil
	//}

	// Apply received CRDT messages in background.
	go s.imapNode.ApplyCRDTUpd(applyCRDTUpdChan, doneCRDTUpdChan)

	return s
}

// findFiles below this node's CRDT root layer.
func (s *service) findFiles() error {

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(s.imapNode.CRDTLayerRoot, "*"))
	if err != nil {
		return fmt.Errorf("[imap.InitWorker] Globbing for CRDT folders of users failed with: %v", err)
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return fmt.Errorf("[imap.InitWorker] Error during stat'ing possible user CRDT folder: %v", err)
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return fmt.Errorf("[imap.InitWorker] Reading CRDT failed: %v", err)
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
					return fmt.Errorf("[imap.InitWorker] Reading CRDT failed: %v", err)
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

// Run loops over incoming requests at worker and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (s *service) Run() error {

	for {

		// Accept request or fail on error.
		conn, err := s.imapNode.MailSocket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at %s failed with: %v", s.Name, err)
		}

		// Dispatch into own goroutine.
		go s.HandleConnection(conn)
	}
}

// HandleConnection is the main worker routine where all
// incoming requests against worker nodes have to go through.
func (s *service) HandleConnection(conn net.Conn) error {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		return fmt.Errorf("[imap.HandleConnection] Worker %s could not convert connection into TLS connection", s.Name)
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

	// TODO: Lock inside that package?!
	//worker.lock.RLock()

	// Extract CRDT and Maildir location for later use.
	CRDTLayerRoot := s.config.CRDTLayerRoot
	MaildirRoot := s.config.MaildirRoot

	// TODO: Lock inside that package?!
	//worker.lock.RUnlock()

	// Based on received client information, update IMAP
	// connection to reflect these information.
	_, err = c.UpdateClientContext(clientInfo, CRDTLayerRoot, MaildirRoot)
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

	// As long as the distributor node did not indicate that
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

		switch req.Command {

		case "SELECT":
			s.Select(c, req, s.SyncSendChan)

		case "CREATE":
			s.Create(c, req, s.SyncSendChan)

		case "DELETE":
			s.Delete(c, req, s.SyncSendChan)

		case "LIST":
			s.List(c, req, s.SyncSendChan)

		case "APPEND":
			s.Append(c, req, s.SyncSendChan)

		case "EXPUNGE":
			s.Expunge(c, req, s.SyncSendChan)

		case "STORE":
			s.Store(c, req, s.SyncSendChan)

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
	if err := c.Terminate(); err != nil {
		return fmt.Errorf("[imap.HandleConnection] Failed to terminate connection: %v", err)
	}

	// Set IMAP state to logged out.
	c.State = imap.Logout

	return nil
}

// Select sets the current mailbox based on supplied
// payload to user-instructed value.
func (s *service) Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.Select(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

// Create attempts to create a mailbox with
// name taken from payload of request.
func (s *service) Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.Create(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

// Delete an existing mailbox with all included content.
func (s *service) Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.Delete(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

// List allows clients to learn about the mailboxes
// available and also returns the hierarchy delimiter.
func (s *service) List(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.List(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

// Append puts supplied message into specified mailbox.
func (s *service) Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.Append(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

// Expunge deletes messages permanently from currently
// selected mailbox that have been flagged as Deleted
// prior to calling this function.
func (s *service) Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.Expunge(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}

// Store takes in message sequence numbers and some set
// of flags to change in those messages and changes the
// attributes for these mails throughout the system.
func (s *service) Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan string) bool {

	ok := s.imapNode.Store(c, req, s.SyncSendChan)
	if ok {

		// If successful, signal end of operation to distributor.
		err := c.SignalSessionDone(true)
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	return ok
}
