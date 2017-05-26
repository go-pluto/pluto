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

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
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

// InternalConnection knows how to create internal tls connections
type InternalConnection interface {
	ReliableConnect(addr string) (*tls.Conn, error)
}

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

	// Proxy forwards one request between the distributor
	// node and the responsible worker node.
	Proxy(c *imap.Connection, rawReq string) bool
}

type service struct {
	authenticator      Authenticator
	internalConnection InternalConnection
	workers            map[string]config.Worker
}

func NewService(authenticator Authenticator, internalConnection InternalConnection, workers map[string]config.Worker) Service {
	return &service{
		authenticator:      authenticator,
		internalConnection: internalConnection,
		workers:            workers,
	}
}

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
				if c.OutConn != nil {

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

		case c.OutConn != nil:
			s.Proxy(c, rawReq)

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
	if c.OutConn != nil {

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

func (s *service) Login(c *imap.Connection, req *imap.Request) bool {

	if c.OutConn != nil {

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

	id, clientID, err := s.authenticator.AuthenticatePlain(userCredentials[0], userCredentials[1], c.IncConn.RemoteAddr().String())
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
	respWorker, err := s.authenticator.GetWorkerForUser(s.workers, id)
	if err != nil {
		c.Error("Authentication error", err)
		return false
	}

	// Store worker connection information.
	workerIP := s.workers[respWorker].PublicIP
	workerPort := s.workers[respWorker].MailPort

	c.OutAddr = fmt.Sprintf("%s:%s", workerIP, workerPort)
	conn, err := s.internalConnection.ReliableConnect(c.OutAddr)
	if err != nil {
		c.Error("Internal connection failure", err)
		return false
	}

	// Save context to connection.
	c.OutConn = conn
	c.OutReader = bufio.NewReader(conn)
	c.ClientID = clientID
	c.UserName = userCredentials[0]

	// Inform worker node about which session just started.
	err = c.SignalSessionStart(false)
	if err != nil {
		c.Error("Encountered send error when distributor was signalling context to worker", err)
		return false
	}

	// Signal success to client.
	err = c.Send(true, fmt.Sprintf("%s OK LOGIN completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

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

func (s *service) Proxy(c *imap.Connection, rawReq string) bool {

	// Pass message to worker node.
	err := c.InternalSend(false, rawReq)
	if err != nil {
		c.Error("Could not proxy request to worker", err)
		return false
	}

	// Reserve space for answer buffer.
	bufResp := make([]string, 0, 6)

	// Receive incoming worker response.
	curResp, err := c.InternalReceive(false)
	if err != nil {
		c.Error("Failed to receive worker's response to proxied command", err)
		return false
	}

	// As long as the responsible worker has not
	// indicated the end of the current operation,
	// continue to buffer answers.
	for (curResp != "> done <") && (strings.HasPrefix(curResp, "> literal: ") != true) {

		// Append it to answer buffer.
		bufResp = append(bufResp, curResp)

		// Receive incoming worker response.
		curResp, err = c.InternalReceive(false)
		if err != nil {
			c.Error("Encountered receive error from worker", err)
			return false
		}
	}

	for i := range bufResp {

		// Send all buffered worker answers to client.
		err = c.Send(true, bufResp[i])
		if err != nil {
			c.Error("Encountered send error to client", err)
			return false
		}
	}

	// Special case: We expect literal data in form of a
	// RFC defined mail message.
	if strings.HasPrefix(curResp, "> literal: ") {

		// Strip off left and right elements of signal.
		// This leaves the awaited amount of bytes.
		numBytesString := strings.TrimPrefix(curResp, "> literal: ")
		numBytesString = strings.TrimSuffix(numBytesString, " <")

		// Convert string amount to int.
		numBytes, err := strconv.Atoi(numBytesString)
		if err != nil {
			c.Error("Encountered conversion error for string to int", err)
			return false
		}

		// Reserve space for exact amount of expected data.
		msgBuffer := make([]byte, numBytes)

		// Read in that amount from connection to client.
		_, err = io.ReadFull(c.IncReader, msgBuffer)
		if err != nil {
			c.Error("Encountered error while reading client literal data", err)
			return false
		}

		// Pass on data to worker. Mails have to be ended by
		// newline symbol.
		_, err = fmt.Fprintf(c.OutConn, "%s", msgBuffer)
		if err != nil {
			c.Error("Encountered passing send error to worker", err)
			return false
		}

		// Reserve space for answer buffer.
		bufResp := make([]string, 0, 6)

		// Receive incoming worker response.
		curResp, err := c.InternalReceive(false)
		if err != nil {
			c.Error("Encountered receive error from worker after literal data was sent", err)
			return false
		}

		// As long as the responsible worker has not
		// indicated the end of the current operation,
		// continue to buffer answers.
		for curResp != "> done <" {

			// Append it to answer buffer.
			bufResp = append(bufResp, curResp)

			// Receive incoming worker response.
			curResp, err = c.InternalReceive(false)
			if err != nil {
				c.Error("Encountered receive error from worker after literal data was sent", err)
				return false
			}
		}

		for i := range bufResp {

			// Send all buffered worker answers to client.
			err = c.Send(true, bufResp[i])
			if err != nil {
				c.Error("Encountered send error to client after literal data was sent", err)
				return false
			}
		}
	}

	return true
}
