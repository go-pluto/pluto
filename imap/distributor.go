package imap

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"crypto/tls"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-kit/kit/metrics"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Interfaces

// PlainAuthenticator defines the methods required to
// perform an IMAP AUTH=PLAIN authentication in order
// to reach authenticated state (also LOGIN).
type PlainAuthenticator interface {

	// GetWorkerForUser allows us to route an IMAP request to the
	// worker node responsible for a specific user.
	GetWorkerForUser(workers map[string]config.Worker, id int) (string, error)

	// AuthenticatePlain will be implemented by each of the
	// authentication methods of type PLAIN to perform the
	// actual part of checking supplied credentials.
	AuthenticatePlain(username string, password string, clientAddr string) (int, string, error)
}

// Structs

type DistributorMetrics struct {
	Commands metrics.Counter
}

// Distributor struct bundles information needed in
// operation of a distributor node.
type Distributor struct {
	lock          *sync.RWMutex
	logger        log.Logger
	Socket        net.Listener
	IntlTLSConfig *tls.Config
	AuthAdapter   PlainAuthenticator
	Connections   map[string]*tls.Conn
	Config        *config.Config
	Metrics       DistributorMetrics
}

// Functions

// InitDistributor listens for TLS connections on a TCP socket
// opened up on supplied IP address and port as well as initializes
// connections to involved worker nodes. It returns those
// information bundled in above Distributor struct.
func InitDistributor(logger log.Logger, metrics DistributorMetrics, config *config.Config, auth PlainAuthenticator) (*Distributor, error) {

	var err error

	// Initialize and set fields.
	distr := &Distributor{
		lock:        new(sync.RWMutex),
		logger:      logger,
		AuthAdapter: auth,
		Connections: make(map[string]*tls.Conn),
		Config:      config,
		Metrics:     metrics,
	}

	// Load internal TLS config.
	distr.IntlTLSConfig, err = crypto.NewInternalTLSConfig(config.Distributor.InternalTLS.CertLoc, config.Distributor.InternalTLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Load public TLS config based on config values.
	publicTLSConfig, err := crypto.NewPublicTLSConfig(config.Distributor.PublicTLS.CertLoc, config.Distributor.PublicTLS.KeyLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming public connections on defined IP and port.
	distr.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Distributor.ListenIP, config.Distributor.Port), publicTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitDistributor] Listening for public TLS connections failed with: %v", err)
	}

	level.Info(logger).Log(
		"msg", "listening for incoming IMAP requests",
		"addr", distr.Socket.Addr().String(),
	)

	return distr, nil
}

// Run loops over incoming requests at distributor and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (distr *Distributor) Run() error {

	for {

		// Accept request or fail on error.
		conn, err := distr.Socket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at distributor failed with: %v", err)
		}

		// Dispatch into own goroutine.
		go distr.HandleConnection(conn)
	}
}

// HandleConnection acts as the main loop for requests
// targeted at IMAP functions implemented in distributor node.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (distr *Distributor) HandleConnection(conn net.Conn) {

	// Assert we are talking via a TLS connection.
	tlsConn, ok := conn.(*tls.Conn)
	if ok != true {
		level.Warn(distr.logger).Log("msg", "distributor could not convert connection into TLS connection")
		return
	}

	// Create a new connection struct for incoming request.
	c := &Connection{
		IncConn:   tlsConn,
		IncReader: bufio.NewReader(tlsConn),
	}

	// Send initial server greeting.
	err := c.Send(true, fmt.Sprintf("* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN] %s", distr.Config.IMAP.Greeting))
	if err != nil {
		c.Error("Encountered error while sending IMAP greeting", err)
		return
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
						return
					}
				}

				// And terminate connection.
				c.Terminate()

			} else {
				// If not a simple disconnect, log error and
				// terminate connection to client.
				c.Error("Encountered receive error from client", err)
			}

			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(true, err.Error())
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		switch {

		case req.Command == "CAPABILITY":
			distr.Capability(c, req)
			distr.Metrics.Commands.With("capability").Add(1)

		case req.Command == "LOGOUT":
			if ok := distr.Logout(c, req); ok {
				// A LOGOUT marks connection termination.
				recvUntil = "LOGOUT"
				distr.Metrics.Commands.With("logout").Add(1)
			}

		case req.Command == "STARTTLS":
			distr.StartTLS(c, req)
			distr.Metrics.Commands.With("starttls").Add(1)

		case req.Command == "LOGIN":
			distr.Login(c, req)
			distr.Metrics.Commands.With("login").Add(1)

		case c.OutConn != nil:
			distr.Proxy(c, rawReq)

		default:
			// Client sent inappropriate command. Signal tagged error.
			distr.Metrics.Commands.With("error").Add(1)
			err := c.Send(true, fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}

	// Terminate connection after logout.
	if err := c.Terminate(); err != nil {
		level.Warn(distr.logger).Log(
			"msg", "failed to terminate connection",
			"err", err,
		)
	}
}
