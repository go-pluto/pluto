package imap

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"

	"github.com/numbleroot/pluto/auth"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Distributor struct bundles information needed in
// operation of a distributor node.
type Distributor struct {
	Socket      net.Listener
	AuthAdapter auth.PlainAuthenticator
	Connections map[string]*tls.Conn
	Config      *config.Config
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
		Connections: make(map[string]*tls.Conn),
		Config:      config,
	}

	// As the distributor is responsible for the authentication
	// of incoming requests, connect to provided auth mechanism.
	if config.Distributor.AuthAdapter == "AuthFile" {

		// Open authentication file and read user information.
		distr.AuthAdapter, err = auth.NewFileAuthenticator(config.Distributor.AuthFile.File, config.Distributor.AuthFile.Separator)
		if err != nil {
			return nil, err
		}
	}

	// Load internal TLS config.
	internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Distributor.InternalTLS.CertLoc, config.Distributor.InternalTLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Connect to all worker nodes in order to already
	// have established TLS connections later on.
	for name, worker := range config.Workers {

		// Try to connect to IMAP port of worker with internal TLS config.
		c, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", worker.IP, worker.MailPort), internalTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitDistributor] Could not connect to %s because of: %s\n", name, err.Error())
		}

		// Save connection for later use.
		distr.Connections[name] = c
	}

	// Load public TLS config based on config values.
	publicTLSConfig, err := crypto.NewPublicTLSConfig(config.Distributor.PublicTLS.CertLoc, config.Distributor.PublicTLS.KeyLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming public connections on defined IP and port.
	distr.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Distributor.IP, config.Distributor.Port), publicTLSConfig)
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

	// Create a new connection struct for incoming request.
	c := NewConnection(conn)

	// Send initial server greeting.
	err := c.Send(fmt.Sprintf("* OK IMAP4rev1 %s", distr.Config.IMAP.Greeting))
	if err != nil {
		c.Error("Encountered error while sending IMAP greeting", err)
		return
	}

	// As long as we do not receive a LOGOUT
	// command from client, we accept requests.
	recvUntil := ""

	for recvUntil != "LOGOUT" {

		// Receive next incoming client command.
		rawReq, err := c.Receive()
		if err != nil {

			// Check if error was a simple disconnect.
			if err.Error() == "EOF" {

				// If so and if a worker was already assigned,
				// inform the worker about the disconnect.
				if c.Worker != "" {

					// Prefix the information with context.
					if err := c.SignalSessionPrefix(distr.Connections[c.Worker]); err != nil {
						c.Error("Encountered send error when distributor signalled context to worker", err)
						return
					}

					// Now signal that client disconnected.
					if err := c.SignalSessionDone(distr.Connections[c.Worker]); err != nil {
						c.Error("Encountered send error when distributor signalled end to worker", err)
						return
					}
				}

				// And terminate connection.
				c.Terminate()

			} else {
				// If not a simple disconnect, log error and
				// terminate connection to client.
				c.Error("Encountered receive error", err)
			}

			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
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

		case req.Command == "LOGIN":
			distr.Login(c, req)

		case req.Command == "LOGOUT":
			if ok := distr.Logout(c, req); ok {
				// A LOGOUT marks connection termination.
				recvUntil = "LOGOUT"
			}

		case req.Command == "STARTTLS":
			distr.StartTLS(c, req)

		case c.Worker != "":
			distr.Proxy(c, rawReq)

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}
}
