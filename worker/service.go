package worker

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/imap"
	"google.golang.org/grpc"
)

// Service defines the interface a worker node
// in a pluto network provides.
type Service interface {

	// Init initializes node-type specific fields.
	Init(syncSendChan chan comm.Msg) error

	// ApplyCRDTUpd receives strings representing CRDT
	// update operations from receiver and executes them.
	ApplyCRDTUpd(applyCRDTUpd chan comm.Msg, doneCRDTUpd chan struct{})

	// Run loops over incoming requests at worker and
	// dispatches each one to a goroutine taking care of
	// the commands supplied.
	Run() error

	// Select sets the current mailbox based on supplied
	// payload to user-instructed value.
	Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool

	// Create attempts to create a mailbox with
	// name taken from payload of request.
	Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool

	// Delete an existing mailbox with all included content.
	Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool

	// List allows clients to learn about the mailboxes
	// available and also returns the hierarchy delimiter.
	List(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool

	// Append puts supplied message into specified mailbox.
	Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool

	// Expunge deletes messages permanently from currently
	// selected mailbox that have been flagged as Deleted
	// prior to calling this function.
	Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool

	// Store takes in message sequence numbers and some set
	// of flags to change in those messages and changes the
	// attributes for these mails throughout the system.
	Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool
}

type service struct {
	imapNode     *imap.IMAPNode
	tlsConfig    *tls.Config
	config       config.Worker
	Name         string
	IMAPNodeGRPC *grpc.Server
	SyncSendChan chan comm.Msg
}

// NewService takes in all required parameters for spinning
// up a new worker node, runs initialization code, and returns
// a service struct for this node type wrapping all information.
func NewService(tlsConfig *tls.Config, config config.Config, name string) Service {

	return &service{
		imapNode: &imap.IMAPNode{
			Lock:               &sync.RWMutex{},
			MailboxStructure:   make(map[string]map[string]*crdt.ORSet),
			MailboxContents:    make(map[string]map[string][]string),
			CRDTLayerRoot:      config.CRDTLayerRoot,
			MaildirRoot:        config.MaildirRoot,
			HierarchySeparator: config.IMAP.HierarchySeparator,
		},
		tlsConfig: tlsConfig,
		config:    config.Workers[name],
		Name:      name,
	}
}

// Init executes functions organizing files and folders
// needed for this node and passes on the synchronization
// channel to the service.
func (s *service) Init(syncSendChan chan comm.Msg) error {

	err := s.findFiles()
	if err != nil {
		return err
	}

	s.SyncSendChan = syncSendChan

	// Define options for an empty gRPC server.
	options := imap.NodeOptions(s.tlsConfig)
	s.IMAPNodeGRPC = grpc.NewServer(options...)

	// Register the empty server on fulfilling interface.
	imap.RegisterNodeServer(s.IMAPNodeGRPC, s)

	return err
}

// findFiles below this node's CRDT root layer.
func (s *service) findFiles() error {

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(s.imapNode.CRDTLayerRoot, "*"))
	if err != nil {
		return fmt.Errorf("[imap.initWorker] Globbing for CRDT folders of users failed with: %v", err)
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return fmt.Errorf("[imap.initWorker] Error during stat'ing possible user CRDT folder: %v", err)
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return fmt.Errorf("[imap.initWorker] Reading CRDT failed: %v", err)
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
					return fmt.Errorf("[imap.initWorker] Reading CRDT failed: %v", err)
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

// ApplyCRDTUpd passes on the required arguments for
// invoking the IMAP node's ApplyCRDTUpd function so
// that CRDT messages will get applied in background.
func (s *service) ApplyCRDTUpd(applyCRDTUpd chan comm.Msg, doneCRDTUpd chan struct{}) {
	s.imapNode.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)
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
		go s.handleConnection(conn)
	}
}

// handleConnection is the main worker routine where all
// incoming requests against worker nodes have to go through.
func (s *service) handleConnection(conn net.Conn) error {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		return fmt.Errorf("[imap.handleConnection] Worker %s could not convert connection into TLS connection", s.Name)
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

	// Extract CRDT and Maildir location for later use.
	CRDTLayerRoot := s.config.CRDTLayerRoot
	MaildirRoot := s.config.MaildirRoot

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
		return fmt.Errorf("[imap.handleConnection] Failed to terminate connection: %v", err)
	}

	// Set IMAP state to logged out.
	c.State = imap.Logout

	return nil
}

// Select sets the current mailbox based on supplied
// payload to user-instructed value.
func (s *service) Select(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
func (s *service) Create(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
func (s *service) Delete(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
func (s *service) List(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
func (s *service) Append(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
func (s *service) Expunge(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
func (s *service) Store(c *imap.IMAPConnection, req *imap.Request, syncChan chan comm.Msg) bool {

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
