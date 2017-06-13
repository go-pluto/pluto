package distributor

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"crypto/tls"

	"github.com/golang/protobuf/proto"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Interfaces

// Authenticator defines the methods required to
// perform an IMAP AUTH=PLAIN authentication in order
// to reach authenticated state (also LOGIN).
type Authenticator interface {

	// GetWorkerForUser allows us to route an IMAP request to the
	// worker node responsible for a specific user.
	GetWorkerForUser(workers map[string]config.Worker, id int) (string, error)

	// AuthenticatePlain will be implemented by each of the
	// authentication methods of type PLAIN to perform the
	// actual part of checking supplied credentials.
	AuthenticatePlain(username string, password string, clientAddr string) (int, string, error)
}

// Service defines the interface a distributor node
// in a pluto network provides.
type Service interface {

	// Run loops over incoming requests at distributor and
	// dispatches each one to a goroutine taking care of
	// the commands supplied.
	Run(net.Listener, string) error

	// Capability handles the IMAP CAPABILITY command.
	// It outputs the supported actions in the current state.
	Capability(c *imap.Connection, req *imap.Request) bool

	// Logout correctly ends a connection with a client.
	// Also necessarily created management structures will
	// get shut down gracefully.
	Logout(c *imap.Connection, req *imap.Request) bool

	// Login performs the authentication mechanism specified
	// as part of the distributor config.
	Login(c *imap.Connection, req *imap.Request) bool

	// StartTLS states on IMAP STARTTLS command
	// that current connection is already encrypted.
	StartTLS(c *imap.Connection, req *imap.Request) bool

	// ProxySelect tunnels a received SELECT request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxySelect(c *imap.Connection, rawReq string) bool

	// ProxyCreate tunnels a received CREATE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyCreate(c *imap.Connection, rawReq string) bool

	// ProxyDelete tunnels a received DELETE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyDelete(c *imap.Connection, rawReq string) bool

	// ProxyList tunnels a received LIST request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyList(c *imap.Connection, rawReq string) bool

	// ProxyAppend tunnels a received APPEND request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyAppend(c *imap.Connection, rawReq string) bool

	// ProxyExpunge tunnels a received EXPUNGE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyExpunge(c *imap.Connection, rawReq string) bool

	// ProxyStore tunnels a received STORE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyStore(c *imap.Connection, rawReq string) bool
}

type service struct {
	auther      Authenticator
	tlsConfig   *tls.Config
	workers     map[string]config.Worker
	gRPCOptions []grpc.DialOption
}

// NewService takes in all required parameters for spinning
// up a new distributor node and returns a service struct for
// this node type wrapping all information.
func NewService(auther Authenticator, tlsConfig *tls.Config, workers map[string]config.Worker) Service {

	return &service{
		auther:      auther,
		tlsConfig:   tlsConfig,
		workers:     workers,
		gRPCOptions: DistributorOptions(tlsConfig),
	}
}

// Run loops over incoming requests at distributor and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (s *service) Run(listener net.Listener, greeting string) error {

	for {
		// Accept request or fail on error.
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at distributor failed with: %v", err)
		}

		// Dispatch into own goroutine.
		go s.handleConnection(conn, greeting)
	}
}

// handleConnection performs the main actions on public
// connections to a pluto system. It aggregates context,
// invokes correct methods for supplied IMAP commands, and
// proxies state-changing requests to the responsible worker
// or storage node (failover).
func (s *service) handleConnection(conn net.Conn, greeting string) error {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		return errors.New("connection is no *tls.Conn")
	}

	// Create a new connection struct for incoming request.
	c := &imap.Connection{
		IncConn:   tlsConn,
		IncReader: bufio.NewReader(tlsConn),
	}

	// Send initial server greeting.
	err := c.Send(true, fmt.Sprintf("* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN] %s", greeting))
	if err != nil {
		c.Error("Encountered error while sending IMAP greeting", err)
		return err
	}

	// As long as we do not receive a LOGOUT
	// command from client, we accept requests.
	recvUntil := ""

	for recvUntil != "LOGOUT" {

		// Receive next incoming client command.
		rawReq, err := c.Receive(true)
		if err != nil {

			// Check if error was a simple disconnect.
			if err.Error() == "EOF" {

				// If so and if a worker was already assigned,
				// inform the worker about the disconnect.
				if c.IsAuthorized {

					// Signal to worker node that session is done.
					err := c.SignalSessionDone(false)
					if err != nil {
						c.Error("Encountered send error when distributor was signalling end to worker", err)
						return err
					}
				}

				// And terminate connection.
				c.Terminate()

			} else {
				// If not a simple disconnect, log error and
				// terminate connection to client.
				c.Error("Encountered receive error from client", err)
			}

			return err
		}

		// Parse received next raw request into struct.
		req, err := imap.ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(true, err.Error())
			if err != nil {
				c.Error("Encountered send error", err)
				return err
			}

			// Go back to beginning of loop.
			continue
		}

		switch {

		case req.Command == "CAPABILITY":
			s.Capability(c, req)

		case req.Command == "LOGOUT":
			if ok := s.Logout(c, req); ok {
				// A LOGOUT marks connection termination.
				recvUntil = "LOGOUT"
			}

		case req.Command == "STARTTLS":
			s.StartTLS(c, req)

		case req.Command == "LOGIN":
			s.Login(c, req)

		case (c.IsAuthorized) && (req.Command == "SELECT"):
			s.ProxySelect(c, req)

		case (c.IsAuthorized) && (req.Command == "CREATE"):
			s.ProxyCreate(c, req)

		case (c.IsAuthorized) && (req.Command == "DELETE"):
			s.ProxyDelete(c, req)

		case (c.IsAuthorized) && (req.Command == "LIST"):
			s.ProxyList(c, req)

		case (c.IsAuthorized) && (req.Command == "APPEND"):
			s.ProxyAppend(c, req)

		case (c.IsAuthorized) && (req.Command == "EXPUNGE"):
			s.ProxyExpunge(c, req)

		case (c.IsAuthorized) && (req.Command == "STORE"):
			s.ProxyStore(c, req)

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(true, fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return err
			}
		}
	}

	// Terminate connection after logout.
	return c.Terminate()
}

// Capability handles the IMAP CAPABILITY command.
// It outputs the supported actions in the current state.
func (s *service) Capability(c *imap.Connection, req *imap.Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to CAPABILITY command,
		// this is a client error. Return BAD statement.
		err := c.Send(true, fmt.Sprintf("%s BAD Command CAPABILITY was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Send mandatory capability options.
	// This means, AUTH=PLAIN is allowed and nothing else.
	// STARTTLS will be answered but is not listed as
	// each connection already is a TLS connection.
	err := c.Send(true, fmt.Sprintf("* CAPABILITY IMAP4rev1 AUTH=PLAIN\r\n%s OK CAPABILITY completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// TODO: Change returned capabilities based on IMAP state of
	//       connection, e.g. more capabilities if authenticated.

	return true
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (s *service) Logout(c *imap.Connection, req *imap.Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to LOGOUT command,
		// this is a client error. Return BAD statement.
		err := c.Send(true, fmt.Sprintf("%s BAD Command LOGOUT was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// If already a worker was assigned, signal logout.
	if c.IsAuthorized {

		// Signal to worker node that session is done.
		err := c.SignalSessionDone(false)
		if err != nil {
			c.Error("Encountered send error when distributor was signalling end to worker", err)
			return false
		}
	}

	// Signal success to client.
	err := c.Send(true, fmt.Sprintf("* BYE Terminating connection\r\n%s OK LOGOUT completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Login performs the authentication mechanism specified
// as part of the distributor config.
func (s *service) Login(c *imap.Connection, req *imap.Request) bool {

	if c.IsAuthorized {

		// Connection was already once authenticated,
		// cannot do that a second time, client error.
		// Send tagged BAD response.
		err := c.Send(true, fmt.Sprintf("%s BAD Command LOGIN cannot be executed in this state", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every space character.
	userCredentials := strings.Split(req.Payload, " ")

	if len(userCredentials) != 2 {

		// If payload did not contain exactly two elements,
		// this is a client error. Return BAD statement.
		err := c.Send(true, fmt.Sprintf("%s BAD Command LOGIN was not sent with exactly two parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Perform the actual authentication.
	id, clientID, err := s.auther.AuthenticatePlain(userCredentials[0], userCredentials[1], c.IncConn.RemoteAddr().String())
	if err != nil {

		// If supplied credentials failed to authenticate client,
		// they are invalid. Return NO statement.
		err := c.Send(true, fmt.Sprintf("%s NO Name and / or password wrong", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Find worker node responsible for this connection.
	respWorker, err := s.auther.GetWorkerForUser(s.workers, id)
	if err != nil {
		c.Error("Authentication error", err)
		return false
	}

	// Connect to responsible worker or storage node.
	conn, err := grpc.Dial(s.workers[respWorker].PublicMailAddr, s.gRPCOptions...)
	if err != nil {
		c.Error(fmt.Sprintf("could not connect to internal node %s", respWorker), err)
		return false
	}

	// Save context to connection struct.
	c.gRPCClient = imap.NewNodeClient(conn)
	c.IsAuthorized = true
	c.ClientID = clientID
	c.UserName = userCredentials[0]

	// Signal success to client.
	err = c.Send(true, fmt.Sprintf("%s OK LOGIN completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// StartTLS states on IMAP STARTTLS command
// that current connection is already encrypted.
func (s *service) StartTLS(c *imap.Connection, req *imap.Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to STARTTLS command,
		// this is a client error. Return BAD statement.
		err := c.Send(true, fmt.Sprintf("%s BAD Command STARTTLS was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// As the connection is already TLS encrypted,
	// tell client that a TLS session is active.
	err := c.Send(true, fmt.Sprintf("%s BAD TLS is already active", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// ProxySelect tunnels a received SELECT request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxySelect(c *imap.Connection, rawReq string) bool {

	// Send the request via gRPC.
	resp, err := c.gRPCClient.Select(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy SELECT", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending SELECT answer to client", err)
		return false
	}

	return true
}

// ProxyCreate tunnels a received CREATE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyCreate(c *imap.Connection, rawReq string) bool {

	// Send the request via gRPC.
	resp, err := c.gRPCClient.Create(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy CREATE", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending CREATE answer to client", err)
		return false
	}

	return true
}

// ProxyDelete tunnels a received DELETE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyDelete(c *imap.Connection, rawReq string) bool {

	// Send the request via gRPC.
	resp, err := c.gRPCClient.Delete(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy DELETE", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending DELETE answer to client", err)
		return false
	}

	return true
}

// ProxyList tunnels a received LIST request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyList(c *imap.Connection, rawReq string) bool {

	// Send the request via gRPC.
	resp, err := c.gRPCClient.List(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy LIST", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending LIST answer to client", err)
		return false
	}

	return true
}

// ProxyAppend tunnels a received APPEND request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyAppend(c *imap.Connection, rawReq string) bool {

	// Send the initial request via gRPC.
	resp, err := c.gRPCClient.Append(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy IMAP part of APPEND", err)
		return false
	}

	// Pass on either error or continuation response to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending APPEND answer to client", err)
		return false
	}

	// Check if seen response was no continuation response.
	// In such case, simply return as this function is done here.
	if resp.Text != "+ Ready for literal data" {
		return true
	}

	// Reserve space for exact amount of expected data.
	msgBuffer := make([]byte, resp.IsAppend.AwaitedNumBytes)

	// Read in that amount from connection to client.
	_, err = io.ReadFull(c.IncReader, msgBuffer)
	if err != nil {
		c.Error("Encountered error while reading client literal data", err)
		return false
	}

	// Send the request via gRPC.
	resp, err = c.gRPCClient.Append(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
		HasMsg: *Command_Message{
			Content: msgBuffer,
		},
	})
	if err != nil {
		c.Error("failed to proxy message contained in APPEND", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending APPEND answer to client", err)
		return false
	}

	return true
}

// ProxyExpunge tunnels a received EXPUNGE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyExpunge(c *imap.Connection, rawReq string) bool {

	// Send the request via gRPC.
	resp, err := c.gRPCClient.Expunge(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy EXPUNGE", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending EXPUNGE answer to client", err)
		return false
	}

	return true
}

// ProxyStore tunnels a received STORE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyStore(c *imap.Connection, rawReq string) bool {

	// Send the request via gRPC.
	resp, err := c.gRPCClient.Store(context.Background(), Command{
		Text:     rawReq,
		ClientID: c.ClientID,
		UserName: c.UserName,
	})
	if err != nil {
		c.Error("failed to proxy STORE", err)
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(true, resp.Text)
	if err != nil {
		c.Error("error while sending STORE answer to client", err)
		return false
	}

	return true
}
