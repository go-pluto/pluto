package worker

import (
	"fmt"
	"net"
	"os"
	"sync"

	"crypto/tls"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-pluto/pluto/comm"
	"github.com/go-pluto/pluto/config"
	"github.com/go-pluto/pluto/crdt"
	"github.com/go-pluto/pluto/imap"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Structs

// Metrics has all metrics exposed by a worker.
type Metrics struct {
	Labels      []string
	VectorClock *prometheus.CounterVec
}

type service struct {
	imapNode     *imap.IMAPNode
	mailboxes    map[string]*imap.Mailbox
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
	Init(logger log.Logger, sep string, syncSendChan chan comm.Msg) error

	// ApplyCRDTUpd receives strings representing CRDT
	// update operations from receiver and executes them.
	ApplyCRDTUpd(applyCRDTUpd <-chan comm.Msg, doneCRDTUpd chan<- struct{})

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

	// AppendAbort removes meta data tracking an in-progress
	// APPEND command from an internal node in case of client error.
	AppendAbort(ctx context.Context, abort *imap.Abort) (*imap.Confirmation, error)

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
func NewService(name string, tlsConfig *tls.Config, config *config.Config) Service {

	return &service{
		mailboxes: make(map[string]*imap.Mailbox),
		tlsConfig: tlsConfig,
		config:    config.Workers[name],
		sessions:  make(map[string]*imap.Session),
		Name:      name,
	}
}

// Init executes functions organizing files and folders
// needed for this node and passes on the synchronization
// channel to the service.
func (s *service) Init(logger log.Logger, sep string, syncSendChan chan comm.Msg) error {

	// Build internal CRDT state.
	err := s.constructState(logger, sep)
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

// constructState reads in each user's structure
// CRDT and builds an internal state representation
// from found information.
func (s *service) constructState(logger log.Logger, sep string) error {

	// Find all files below this node's CRDT root layer.
	folders, err := filepath.Glob(filepath.Join(s.config.CRDTLayerRoot, "*"))
	if err != nil {
		return fmt.Errorf("globbing for CRDT folders of users failed with: %v", err)
	}

	for _, folder := range folders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return fmt.Errorf("error during stat'ing possible user CRDT folder: %v", err)
		}

		// Only consider folders for building CRDT state.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			structureCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "structure.crdt"))
			if err != nil {
				return fmt.Errorf("reading structure CRDT failed: %v", err)
			}

			s.mailboxes[userName] = &imap.Mailbox{
				Logger:             logger,
				Lock:               &sync.RWMutex{},
				Structure:          structureCRDT,
				Mails:              make(map[string][]string),
				CRDTPath:           filepath.Join(s.config.CRDTLayerRoot, userName),
				MaildirPath:        filepath.Join(s.config.MaildirRoot, userName),
				HierarchySeparator: sep,
			}

			// Retrieve the names of all mailbox folders
			// this user has present in the mailbox.
			mailboxFolders := structureCRDT.GetAllValues()

			var mailboxFolderCur string
			for _, mailboxFolder := range mailboxFolders {

				// Prepare some space for found mail files.
				s.mailboxes[userName].Mails[mailboxFolder] = make([]string, 0, 6)

				if mailboxFolder == "INBOX" {
					mailboxFolderCur = filepath.Join(s.config.MaildirRoot, userName, "cur")
				} else {
					mailboxFolderCur = filepath.Join(s.config.MaildirRoot, userName, mailboxFolder, "cur")
				}

				// Read file system content (mail messages)
				// into internal state.
				err := filepath.Walk(mailboxFolderCur, func(path string, info os.FileInfo, err error) error {

					if err != nil {
						return err
					}

					// Do not consider the "cur" folder itself.
					if path == mailboxFolderCur {
						return nil
					}

					// If we found a mail file, append it to the internal list.
					if info.Mode().IsRegular() {
						s.mailboxes[userName].Mails[mailboxFolder] = append(s.mailboxes[userName].Mails[mailboxFolder], info.Name())
					}

					return nil
				})
				if err != nil {
					return fmt.Errorf("error while walking user Maildir: %v", err)
				}
			}
		}
	}

	return nil
}

// ApplyCRDTUpd receives strings representing CRDT
// update operations from receiver and executes them.
func (s *service) ApplyCRDTUpd(applyCRDTUpd <-chan comm.Msg, doneCRDTUpd chan<- struct{}) {

	for {

		// Receive update message from receiver
		// via channel.
		msg := <-applyChan

		// Depending on received operation,
		// parse remaining payload further.
		switch msg.Operation {

		case "create":

			// Based on specified user in message,
			// select correct mailbox to manipulate.
			mailbox := s.mailboxes[msg.Create.User]

			// Execute authoritative function to
			// apply received updates.
			mailbox.ApplyCreate(msg.Create)

		case "delete":
			mailbox := s.mailboxes[msg.Delete.User]
			mailbox.ApplyDelete(msg.Delete)

		case "append":
			mailbox := s.mailboxes[msg.Append.User]
			mailbox.ApplyAppend(msg.Append)

		case "expunge":
			mailbox := s.mailboxes[msg.Expunge.User]
			mailbox.ApplyExpunge(msg.Expunge)

		case "store":
			mailbox := s.mailboxes[msg.Store.User]
			mailbox.ApplyStore(msg.Store)
		}

		// Signal receiver that update was performed.
		doneChan <- struct{}{}
	}
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
		State:             imap.StateAuthenticated,
		ClientID:          clientCtx.ClientID,
		UserName:          clientCtx.UserName,
		RespWorker:        clientCtx.RespWorker,
		StorageSubnetChan: nil,
		UserCRDTPath:      filepath.Join(s.config.CRDTLayerRoot, clientCtx.UserName),
		UserMaildirPath:   filepath.Join(s.config.MaildirRoot, clientCtx.UserName),
		AppendInProg:      nil,
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

// AppendAbort removes meta data tracking an in-progress
// APPEND command from an internal node in case of client error.
func (s *service) AppendAbort(ctx context.Context, abort *imap.Abort) (*imap.Confirmation, error) {

	// Retrieve active IMAP connection context
	// from map of all known to this node.
	// Note: ClientID is expected to truly identify
	// exactly one device session (thus, no locking).
	sess := s.sessions[abort.ClientID]

	// Remove in-progress meta data.
	sess.AppendInProg = nil
	s.imapNode.Lock.Unlock()

	return &imap.Confirmation{
		Status: 0,
	}, nil
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
