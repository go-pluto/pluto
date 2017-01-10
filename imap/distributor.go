package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"

	"crypto/tls"

	"github.com/numbleroot/pluto/auth"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Distributor struct bundles information needed in
// operation of a distributor node.
type Distributor struct {
	lock          *sync.RWMutex
	Socket        net.Listener
	IntlTLSConfig *tls.Config
	AuthAdapter   auth.PlainAuthenticator
	Connections   map[string]*tls.Conn
	Config        *config.Config
}

// Functions

// InitDistributor listens for TLS connections on a TCP socket
// opened up on supplied IP address and port as well as initializes
// connections to involved worker nodes. It returns those
// information bundeled in above Distributor struct.
func InitDistributor(config *config.Config) (*Distributor, error) {

	var err error

	// Initialize and set fields.
	distr := &Distributor{
		lock:        new(sync.RWMutex),
		Connections: make(map[string]*tls.Conn),
		Config:      config,
	}

	// As the distributor is responsible for the authentication
	// of incoming requests, connect to provided auth mechanism.
	if config.Distributor.AuthAdapter == "AuthFile" {

		// Open authentication file and read user information.
		distr.AuthAdapter, err = auth.NewFileAuthenticator(config.Distributor.AuthFile.File, config.Distributor.AuthFile.Separator)

	} else if config.Distributor.AuthAdapter == "AuthPostgres" {

		// Connect to PostgreSQL database.
		distr.AuthAdapter, err = auth.NewPostgresAuthenticator(config.Distributor.AuthPostgres.IP, config.Distributor.AuthPostgres.Port, config.Distributor.AuthPostgres.Database, config.Distributor.AuthPostgres.User, config.Distributor.AuthPostgres.Password, config.Distributor.AuthPostgres.UseTLS)

	}
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("[imap.InitDistributor] Listening for public TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitDistributor] Listening for incoming IMAP requests on %s.\n", distr.Socket.Addr())

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
			return fmt.Errorf("[imap.Run] Accepting incoming request at distributor failed with: %s\n", err.Error())
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
		log.Printf("[imap.HandleConnection] Distributor could not convert connection into TLS connection.\n")
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

		case req.Command == "LOGOUT":
			if ok := distr.Logout(c, req); ok {
				// A LOGOUT marks connection termination.
				recvUntil = "LOGOUT"
			}

		case req.Command == "STARTTLS":
			distr.StartTLS(c, req)

		case req.Command == "LOGIN":
			distr.Login(c, req)

		case c.OutConn != nil:
			distr.Proxy(c, rawReq)

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(true, fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}

	// Terminate connection after logout.
	err = c.Terminate()
	if err != nil {
		log.Fatalf("[imap.HandleConnection] Failed to terminate connection: %s\n", err.Error())
	}
}
