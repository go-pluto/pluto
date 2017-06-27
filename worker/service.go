package worker

import (
	"fmt"
	"net"
	"os"
	"sync"

	"crypto/tls"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/imap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Structs

type service struct {
	imapNode     *imap.IMAPNode
	tlsConfig    *tls.Config
	config       config.Worker
	sessions     map[string]*imap.Session
	Name         string
	IMAPNodeGRPC *grpc.Server
	SyncSendChan chan comm.Msg
}

// Interfaces

// Service defines the interface a worker node
// in a pluto network provides.
type Service interface {

	// Init initializes node-type specific fields.
	Init(syncSendChan chan comm.Msg) error

	// ApplyCRDTUpd receives strings representing CRDT
	// update operations from receiver and executes them.
	ApplyCRDTUpd(applyCRDTUpd chan comm.Msg, doneCRDTUpd chan struct{})

	// Serve invokes the main gRPC Serve() function.
	Serve(socket net.Listener) error

	// Prepare initializes context for an upcoming client
	// connection on this node.
	Prepare(ctx context.Context, clientCtx *imap.Context) (*imap.Confirmation, error)

	// Close invalidates an active session and deletes
	// information associated with it.
	Close(ctx context.Context, clientCtx *imap.Context) (*imap.Confirmation, error)

	// Select sets the current mailbox based on supplied
	// payload to user-instructed value.
	Select(ctx context.Context, comd *imap.Command) (*imap.Reply, error)

	// Create attempts to create a mailbox with
	// name taken from payload of request.
	Create(ctx context.Context, comd *imap.Command) (*imap.Reply, error)

	// Delete an existing mailbox with all included content.
	Delete(ctx context.Context, comd *imap.Command) (*imap.Reply, error)

	// List allows clients to learn about the mailboxes
	// available and also returns the hierarchy delimiter.
	List(ctx context.Context, comd *imap.Command) (*imap.Reply, error)

	// AppendBegin checks environment conditions and returns
	// a message specifying the awaited number of bytes.
	AppendBegin(ctx context.Context, comd *imap.Command) (*imap.Await, error)

	// AppendEnd receives the mail file associated with a
	// prior AppendBegin.
	AppendEnd(ctx context.Context, comd *imap.MailFile) (*imap.Reply, error)

	// Expunge deletes messages permanently from currently
	// selected mailbox that have been flagged as Deleted
	// prior to calling this function.
	Expunge(ctx context.Context, comd *imap.Command) (*imap.Reply, error)

	// Store takes in message sequence numbers and some set
	// of flags to change in those messages and changes the
	// attributes for these mails throughout the system.
	Store(ctx context.Context, comd *imap.Command) (*imap.Reply, error)
}

// Functions

// NewService takes in all required parameters for spinning
// up a new worker node, runs initialization code, and returns
// a service struct for this node type wrapping all information.
func NewService(logger log.Logger, tlsConfig *tls.Config, config *config.Config, name string) Service {

	return &service{
		imapNode: &imap.IMAPNode{
			Logger:             logger,
			Lock:               &sync.RWMutex{},
			MailboxStructure:   make(map[string]map[string]*crdt.ORSet),
			MailboxContents:    make(map[string]map[string][]string),
			CRDTLayerRoot:      config.Workers[name].CRDTLayerRoot,
			MaildirRoot:        config.Workers[name].MaildirRoot,
			HierarchySeparator: config.IMAP.HierarchySeparator,
		},
		tlsConfig: tlsConfig,
		config:    config.Workers[name],
		sessions:  make(map[string]*imap.Session),
		Name:      name,
	}
}

// Init executes functions organizing files and folders
// needed for this node and passes on the synchronization
// channel to the service.
func (s *service) Init(syncSendChan chan comm.Msg) error {

	// Find all Maildir and CRDT files for this node.
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

// Serve invokes the main gRPC Serve() function.
func (s *service) Serve(socket net.Listener) error {
	return s.IMAPNodeGRPC.Serve(socket)
}

// Prepare initializes context for an upcoming client
// connection on this node.
func (s *service) Prepare(ctx context.Context, clientCtx *imap.Context) (*imap.Confirmation, error) {

	// Create new connection tracking object.
	s.sessions[clientCtx.ClientID] = &imap.Session{
		State:           imap.Authenticated,
		ClientID:        clientCtx.ClientID,
		UserName:        clientCtx.UserName,
		RespWorker:      clientCtx.RespWorker,
		UserCRDTPath:    filepath.Join(s.config.CRDTLayerRoot, clientCtx.UserName),
		UserMaildirPath: filepath.Join(s.config.MaildirRoot, clientCtx.UserName),
		AppendInProg:    nil,
	}

	return &imap.Confirmation{
		Status: 0,
	}, nil
}

// Close invalidates an active session and deletes
// information associated with it.
func (s *service) Close(ctx context.Context, clientCtx *imap.Context) (*imap.Confirmation, error) {

	// Delete connection-tracking object from sessions map.
	delete(s.sessions, clientCtx.ClientID)

	return &imap.Confirmation{
		Status: 0,
	}, nil
}

// Select sets the current mailbox based on supplied
// payload to user-instructed value.
func (s *service) Select(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Reply{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.Select(sess, req, s.SyncSendChan)

	return reply, err
}

// Create attempts to create a mailbox with
// name taken from payload of request.
func (s *service) Create(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Reply{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.Create(sess, req, s.SyncSendChan)

	return reply, err
}

// Delete an existing mailbox with all included content.
func (s *service) Delete(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Reply{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.Delete(sess, req, s.SyncSendChan)

	return reply, err
}

// List allows clients to learn about the mailboxes
// available and also returns the hierarchy delimiter.
func (s *service) List(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Reply{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.List(sess, req, s.SyncSendChan)

	return reply, err
}

// AppendBegin checks environment conditions and returns
// a message specifying the awaited number of bytes.
func (s *service) AppendBegin(ctx context.Context, comd *imap.Command) (*imap.Await, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Await{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	await, err := s.imapNode.AppendBegin(sess, req)

	return await, err
}

// AppendEnd receives the mail file associated with a
// prior AppendBegin.
func (s *service) AppendEnd(ctx context.Context, mailFile *imap.MailFile) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[mailFile.ClientID]

	// Make sure that an APPEND is actually in progress.
	if sess.AppendInProg == nil {

		return &imap.Reply{
			Status: 1,
		}, fmt.Errorf("no APPEND in progress for client %s but AppendEnd was invoked", mailFile.ClientID)
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.AppendEnd(sess, mailFile.Content, s.SyncSendChan)

	return reply, err
}

// Expunge deletes messages permanently from currently
// selected mailbox that have been flagged as Deleted
// prior to calling this function.
func (s *service) Expunge(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Reply{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.Expunge(sess, req, s.SyncSendChan)

	return reply, err
}

// Store takes in message sequence numbers and some set
// of flags to change in those messages and changes the
// attributes for these mails throughout the system.
func (s *service) Store(ctx context.Context, comd *imap.Command) (*imap.Reply, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[comd.ClientID]

	// Parse received raw request into struct.
	req, err := imap.ParseRequest(comd.Text)
	if err != nil {
		return &imap.Reply{
			Status: 1,
		}, err
	}

	// Forward gathered info to IMAP function.
	reply, err := s.imapNode.Store(sess, req, s.SyncSendChan)

	return reply, err
}
