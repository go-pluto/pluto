package distributor

import (
	"bufio"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"strings"

	"crypto/tls"
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

// Structs

type service struct {
	logger        log.Logger
	authenticator Authenticator
	tlsConfig     *tls.Config
	workers       map[string]config.Worker
	storageAddr   string
	gRPCOptions   []grpc.DialOption
}

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
	Capability(c *Connection, req *imap.Request) bool

	// Logout correctly ends a connection with a client.
	// Also necessarily created management structures will
	// get shut down gracefully.
	Logout(c *Connection, req *imap.Request) bool

	// Login performs the authentication mechanism specified
	// as part of the distributor config.
	Login(c *Connection, req *imap.Request) bool

	// StartTLS states on IMAP STARTTLS command
	// that current connection is already encrypted.
	StartTLS(c *Connection, req *imap.Request) bool

	// ProxySelect tunnels a received SELECT request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxySelect(c *Connection, rawReq string) bool

	// ProxyCreate tunnels a received CREATE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyCreate(c *Connection, rawReq string) bool

	// ProxyDelete tunnels a received DELETE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyDelete(c *Connection, rawReq string) bool

	// ProxyList tunnels a received LIST request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyList(c *Connection, rawReq string) bool

	// ProxyAppend tunnels a received APPEND request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyAppend(c *Connection, rawReq string) bool

	// ProxyExpunge tunnels a received EXPUNGE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyExpunge(c *Connection, rawReq string) bool

	// ProxyStore tunnels a received STORE request by
	// an authorized client to the responsible worker or
	// storage node.
	ProxyStore(c *Connection, rawReq string) bool
}

// Functions

// NewService takes in all required parameters for spinning
// up a new distributor node and returns a service struct for
// this node type wrapping all information.
func NewService(logger log.Logger, authenticator Authenticator, tlsConfig *tls.Config, workers map[string]config.Worker, storageAddr string) Service {

	// Disable logging of gRPC components.
	grpclog.SetLogger(stdlog.New(ioutil.Discard, "", 0))

	return &service{
		logger:        logger,
		authenticator: authenticator,
		tlsConfig:     tlsConfig,
		workers:       workers,
		storageAddr:   storageAddr,
		gRPCOptions:   imap.DistributorOptions(tlsConfig),
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
			return fmt.Errorf("accepting incoming request at distributor failed with: %v", err)
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
func (s *service) handleConnection(conn net.Conn, greeting string) {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		level.Info(s.logger).Log("msg", "connection not accepted because it is no *tls.Conn")
		return
	}

	// Create a new connection struct for incoming request.
	c := &Connection{
		IncConn:    tlsConn,
		IncReader:  bufio.NewReader(tlsConn),
		ClientAddr: tlsConn.RemoteAddr().String(),
	}

	// Send initial server greeting.
	err := c.Send(fmt.Sprintf("* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN] %s", greeting))
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
			"err", err,
		)
		return
	}

	// As long as we did not receive a LOGOUT
	// command from client or experienced an
	// error, we accept requests.
	recvUntil := ""
	cmdOK := false

	for recvUntil != "LOGOUT" {

		// Receive next incoming client command.
		rawReq, err := c.Receive()
		if err != nil {

			// Check if error was a simple disconnect.
			if err.Error() == "EOF" {

				level.Debug(s.logger).Log("msg", fmt.Sprintf("client at %s disconnected", c.ClientAddr))

				// If so and if a node was already assigned,
				// inform the node about the disconnect.
				if c.IsAuthorized {

					// Signal to node that session is done.
					conf, err := c.gRPCClient.Close(context.Background(), &imap.Context{
						ClientID:   c.ClientID,
						UserName:   c.UserName,
						RespWorker: c.PrimaryNode,
					})
					if (err != nil) || (conf.Status != 0) {

						if err != nil {
							level.Error(s.logger).Log(
								"msg", fmt.Sprintf("error sending Close() to internal node %s", c.ActualNode),
								"err", err,
							)
						} else if conf.Status != 0 {
							level.Error(s.logger).Log("msg", fmt.Sprintf("sending Close() to internal node %s returned error code", c.ActualNode))
						}
					}
				}

				c.IncConn.Close()

			} else {
				level.Error(s.logger).Log(
					"msg", fmt.Sprintf("error while receiving text from client %s", c.ClientAddr),
					"err", err,
				)
			}

			return
		}

		// Parse received next raw request into struct.
		req, err := imap.ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				level.Error(s.logger).Log(
					"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
					"err", err,
				)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		switch {

		case req.Command == "CAPABILITY":
			cmdOK = s.Capability(c, req)

		case req.Command == "LOGOUT":
			cmdOK = s.Logout(c, req)
			if cmdOK {
				// A LOGOUT marks connection termination.
				recvUntil = "LOGOUT"
			}

		case req.Command == "STARTTLS":
			cmdOK = s.StartTLS(c, req)

		case req.Command == "LOGIN":
			cmdOK = s.Login(c, req)

		case (c.IsAuthorized) && (req.Command == "SELECT"):
			cmdOK = s.ProxySelect(c, rawReq)

		case (c.IsAuthorized) && (req.Command == "CREATE"):
			cmdOK = s.ProxyCreate(c, rawReq)

		case (c.IsAuthorized) && (req.Command == "DELETE"):
			cmdOK = s.ProxyDelete(c, rawReq)

		case (c.IsAuthorized) && (req.Command == "LIST"):
			cmdOK = s.ProxyList(c, rawReq)

		case (c.IsAuthorized) && (req.Command == "APPEND"):
			cmdOK = s.ProxyAppend(c, rawReq)

		case (c.IsAuthorized) && (req.Command == "EXPUNGE"):
			cmdOK = s.ProxyExpunge(c, rawReq)

		case (c.IsAuthorized) && (req.Command == "STORE"):
			cmdOK = s.ProxyStore(c, rawReq)

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				level.Error(s.logger).Log(
					"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
					"err", err,
				)
				return
			}
		}

		// Executed command above indicated failure in
		// operation. Return from function.
		if !cmdOK {
			break
		}
	}

	c.IncConn.Close()

	return
}

// Capability handles the IMAP CAPABILITY command.
// It outputs the supported actions in the current state.
func (s *service) Capability(c *Connection, req *imap.Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to CAPABILITY command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command CAPABILITY was sent with extra parameters", req.Tag))
		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
				"err", err,
			)
			return false
		}

		return true
	}

	// Send mandatory capability options.
	// This means, AUTH=PLAIN is allowed and nothing else.
	// STARTTLS will be answered but is not listed as
	// each connection already is a TLS connection.
	err := c.Send(fmt.Sprintf("* CAPABILITY IMAP4rev1 AUTH=PLAIN\r\n%s OK CAPABILITY completed", req.Tag))
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (s *service) Logout(c *Connection, req *imap.Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to LOGOUT command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGOUT was sent with extra parameters", req.Tag))
		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
				"err", err,
			)
			return false
		}

		return true
	}

	if c.IsAuthorized {

		// If already a worker was assigned, signal logout.
		conf, err := c.gRPCClient.Close(context.Background(), &imap.Context{
			ClientID:   c.ClientID,
			UserName:   c.UserName,
			RespWorker: c.PrimaryNode,
		})
		if (err != nil) || (conf.Status != 0) {

			c.Send("* BAD Internal server error, sorry. Closing connection.")

			if err != nil {
				level.Error(s.logger).Log(
					"msg", fmt.Sprintf("error sending Close() to internal node %s", c.ActualNode),
					"err", err,
				)
			} else if conf.Status != 0 {
				level.Error(s.logger).Log("msg", fmt.Sprintf("sending Close() to internal node %s returned error code", c.ActualNode))
			}

			return false
		}
	}

	// Signal success to client.
	err := c.Send(fmt.Sprintf("* BYE Terminating connection\r\n%s OK LOGOUT completed", req.Tag))
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// Login performs the authentication mechanism specified
// as part of the distributor config.
func (s *service) Login(c *Connection, req *imap.Request) bool {

	if c.IsAuthorized {

		// Connection was already once authenticated,
		// cannot do that a second time, client error.
		// Send tagged BAD response.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGIN cannot be executed in this state", req.Tag))
		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
				"err", err,
			)
			return false
		}

		return true
	}

	// Split payload on every space character.
	userCredentials := strings.Split(req.Payload, " ")

	if len(userCredentials) != 2 {

		// If payload did not contain exactly two elements,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGIN was not sent with exactly two parameters", req.Tag))
		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
				"err", err,
			)
			return false
		}

		return true
	}

	// Perform the actual authentication.
	id, clientID, err := s.authenticator.AuthenticatePlain(userCredentials[0], userCredentials[1], c.ClientAddr)
	if err != nil {

		// If supplied credentials failed to authenticate client,
		// they are invalid. Return NO statement.
		err := c.Send(fmt.Sprintf("%s NO Name and / or password wrong", req.Tag))
		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
				"err", err,
			)
			return false
		}

		return true
	}

	// Find worker node responsible for this connection.
	respWorker, err := s.authenticator.GetWorkerForUser(s.workers, id)
	if err != nil {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error finding worker for user %s with ID %d", userCredentials[0], id),
			"err", err,
		)
		return false
	}

	// Prepary needed node names and addresses.
	c.PrimaryNode = respWorker
	c.PrimaryAddr = s.workers[respWorker].PublicMailAddr
	c.SecondaryNode = "storage"
	c.SecondaryAddr = s.storageAddr

	// Connect to reachable node.
	c.Connect(s.gRPCOptions, s.logger, false)

	// Save context to connection struct.
	c.IsAuthorized = true
	c.ClientID = clientID
	c.UserName = userCredentials[0]

	// Prepare payload to send.
	payload := &imap.Context{
		ClientID:   c.ClientID,
		UserName:   c.UserName,
		RespWorker: c.PrimaryNode,
	}

	// Send worker or storage context of to-come client connection.
	conf, err := c.gRPCClient.Prepare(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during Prepare(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, false)
			conf, err = c.gRPCClient.Prepare(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending Prepare() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if conf.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending Prepare() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// Signal success to client.
	err = c.Send(fmt.Sprintf("%s OK LOGIN completed", req.Tag))
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// StartTLS states on IMAP STARTTLS command
// that current connection is already encrypted.
func (s *service) StartTLS(c *Connection, req *imap.Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to STARTTLS command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command STARTTLS was sent with extra parameters", req.Tag))
		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
				"err", err,
			)
			return false
		}

		return true
	}

	// As the connection is already TLS encrypted,
	// tell client that a TLS session is active.
	err := c.Send(fmt.Sprintf("%s BAD TLS is already active", req.Tag))
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error while sending text to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxySelect tunnels a received SELECT request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxySelect(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the request via gRPC.
	reply, err := c.gRPCClient.Select(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during ProxySelect(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			reply, err = c.gRPCClient.Select(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending Select() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if reply.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending Select() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending SELECT answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxyCreate tunnels a received CREATE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyCreate(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the request via gRPC.
	reply, err := c.gRPCClient.Create(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during ProxyCreate(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			reply, err = c.gRPCClient.Create(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending Create() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if reply.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending Create() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending CREATE answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxyDelete tunnels a received DELETE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyDelete(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the request via gRPC.
	reply, err := c.gRPCClient.Delete(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during ProxyDelete(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			reply, err = c.gRPCClient.Delete(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending Delete() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if reply.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending Delete() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending DELETE answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxyList tunnels a received LIST request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyList(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the request via gRPC.
	reply, err := c.gRPCClient.List(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during ProxyList(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			reply, err = c.gRPCClient.List(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending List() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if reply.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending List() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending LIST answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxyAppend tunnels a received APPEND request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyAppend(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the begin part of the APPEND request via gRPC.
	await, err := c.gRPCClient.AppendBegin(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during begin part of ProxyAppend(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			await, err = c.gRPCClient.AppendBegin(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending AppendBegin() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if await.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending AppendBegin() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// Pass on either error or continuation response to client.
	err = c.Send(await.Text)
	if err != nil {

		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending begin APPEND answer to client %s", c.ClientAddr),
			"err", err,
		)

		// Signal connected internal node that client aborted APPEND.
		conf, err := c.gRPCClient.AppendAbort(context.Background(), &imap.Abort{
			ClientID: c.ClientID,
		})

		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending AppendAbort() to internal node %s", c.ActualNode),
				"err", err,
			)
		}
		if conf.Status != 0 {
			level.Error(s.logger).Log("msg", fmt.Sprintf("sending AppendAbort() to internal node %s returned error code", c.ActualNode))
		}

		return false
	}

	// Check if seen response was no continuation response.
	// In such case, simply return as this function is done here.
	if await.Text != "+ Ready for literal data" {
		return true
	}

	// Reserve space for exact amount of expected data.
	msgBuffer := make([]byte, await.NumBytes)

	// Read in that amount from connection to client.
	_, err = io.ReadFull(c.IncReader, msgBuffer)
	if err != nil {

		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error reading mail content from client %s", c.ClientAddr),
			"err", err,
		)

		// Signal connected internal node that client aborted APPEND.
		conf, err := c.gRPCClient.AppendAbort(context.Background(), &imap.Abort{
			ClientID: c.ClientID,
		})

		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending AppendAbort() to internal node %s", c.ActualNode),
				"err", err,
			)
		}
		if conf.Status != 0 {
			level.Error(s.logger).Log("msg", fmt.Sprintf("sending AppendAbort() to internal node %s returned error code", c.ActualNode))
		}

		return false
	}

	// Expect trailing '\r\n' after message content.
	empty, err := c.Receive()
	if (err != nil) || (empty != "") {

		if err == nil {
			err = fmt.Errorf("received other message than '\r\n'")
		}

		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("failed at expected '\r\n' after message from %s", c.ClientAddr),
			"err", err,
		)

		// Signal connected internal node that client aborted APPEND.
		conf, err := c.gRPCClient.AppendAbort(context.Background(), &imap.Abort{
			ClientID: c.ClientID,
		})

		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending AppendAbort() to internal node %s", c.ActualNode),
				"err", err,
			)
		}
		if conf.Status != 0 {
			level.Error(s.logger).Log("msg", fmt.Sprintf("sending AppendAbort() to internal node %s returned error code", c.ActualNode))
		}

		return false
	}

	// Send the end part of APPEND request via gRPC.
	reply, err := c.gRPCClient.AppendEnd(context.Background(), &imap.MailFile{
		Content:  msgBuffer,
		ClientID: c.ClientID,
	})
	if (err != nil) || (reply.Status != 0) {

		c.Send("* BAD Internal server error, sorry. Closing connection.")

		if err != nil {
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending AppendEnd() to internal node %s", c.ActualNode),
				"err", err,
			)
		} else if reply.Status != 0 {
			level.Error(s.logger).Log("msg", fmt.Sprintf("sending AppendEnd() to internal node %s returned error code", c.ActualNode))
		}

		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending end APPEND answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxyExpunge tunnels a received EXPUNGE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyExpunge(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the request via gRPC.
	reply, err := c.gRPCClient.Expunge(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during ProxyExpunge(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			reply, err = c.gRPCClient.Expunge(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending Expunge() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if reply.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending Expunge() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending EXPUNGE answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}

// ProxyStore tunnels a received STORE request by
// an authorized client to the responsible worker or
// storage node.
func (s *service) ProxyStore(c *Connection, rawReq string) bool {

	// Prepare payload to send.
	payload := &imap.Command{
		Text:     rawReq,
		ClientID: c.ClientID,
	}

	// Send the request via gRPC.
	reply, err := c.gRPCClient.Store(context.Background(), payload)
	for err != nil {

		// Check received gRPC error.
		stat, ok := status.FromError(err)
		if ok && (stat.Code() == codes.Unavailable) {
			level.Debug(s.logger).Log("msg", fmt.Sprintf("%s (%s) unavailable during ProxyStore(), reconnecting...", c.ActualNode, c.ActualAddr))
			c.Connect(s.gRPCOptions, s.logger, true)
			reply, err = c.gRPCClient.Store(context.Background(), payload)
		} else {
			c.Send("* BAD Internal server error, sorry. Closing connection.")
			level.Error(s.logger).Log(
				"msg", fmt.Sprintf("error sending Store() to internal node %s", c.ActualNode),
				"err", err,
			)
			return false
		}
	}

	if reply.Status != 0 {
		c.Send("* BAD Internal server error, sorry. Closing connection.")
		level.Error(s.logger).Log("msg", fmt.Sprintf("sending Store() to internal node %s returned error code", c.ActualNode))
		return false
	}

	// And send response from worker or storage to client.
	err = c.Send(reply.Text)
	if err != nil {
		level.Error(s.logger).Log(
			"msg", fmt.Sprintf("error sending STORE answer to client %s", c.ClientAddr),
			"err", err,
		)
		return false
	}

	return true
}
