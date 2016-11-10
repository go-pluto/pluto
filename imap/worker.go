package imap

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"

	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Worker struct bundles information needed in
// operation of a worker node.
type Worker struct {
	Name        string
	Socket      net.Listener
	Connections map[string]*tls.Conn
	Config      *config.Config
}

// Functions

// InitWorker listens for TLS connections on a TCP socket
// opened up on supplied IP address and port as well as connects
// to involved storage node. It returns those information bundeled
// in above Worker struct.
func InitWorker(config *config.Config, workerName string) (*Worker, error) {

	var err error

	// Initialize and set fields.
	worker := &Worker{
		Name:        workerName,
		Connections: make(map[string]*tls.Conn),
		Config:      config,
	}

	// Check if supplied worker with workerName actually is configured.
	if _, ok := config.Workers[worker.Name]; !ok {

		var workerID string

		// Retrieve first valid worker ID to provide feedback.
		for workerID = range config.Workers {
			break
		}

		return nil, fmt.Errorf("[imap.InitWorker] Specified worker ID does not exist in config file. Please provide a valid one, for example '%s'.\n", workerID)
	}

	// Load internal TLS config.
	internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Workers[worker.Name].TLS.CertLoc, config.Workers[worker.Name].TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Try to connect to storage node with internal TLS config.
	c, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.Port), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitWorker] Could not connect to storage node because of: %s\n", err.Error())
	}

	// Save connection for later use.
	worker.Connections["storage"] = c

	// Start to listen for incoming internal connections on defined IP and port.
	worker.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[worker.Name].IP, config.Workers[worker.Name].Port), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitWorker] Listening for internal TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitWorker] Listening for incoming IMAP requests on %s.\n", worker.Socket.Addr())

	return worker, nil
}

// Run loops over incoming requests at worker and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (worker *Worker) Run() error {

	for {

		// Accept request or fail on error.
		conn, err := worker.Socket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at %s failed with: %s\n", worker.Name, err.Error())
		}

		// Dispatch into own goroutine.
		go worker.HandleConnection(conn)
	}
}

// HandleConnection is the main worker routine where all
// incoming requests against worker nodes have to go through.
func (worker *Worker) HandleConnection(conn net.Conn) {

	// Create a new connection struct for incoming request.
	c := NewConnection(conn)

	// Receive opening information.
	opening, err := c.Receive()
	if err != nil {
		c.ErrorLogOnly("Encountered receive error", err)
		return
	}

	// As long as the distributor node did not indicate that
	// the system is about to shut down, we accept requests.
	for opening != "> done <" {

		// Extract important parts and inject them into struct.
		context, err := ExtractContext(opening)
		if err != nil {
			c.ErrorLogOnly("Error extracting context", err)
			return
		}

		// Receive incoming actual client command.
		rawReq, err := c.Receive()
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			// In case of failure, wait for next sent command.
			rawReq, err = c.Receive()
			if err != nil {
				c.ErrorLogOnly("Encountered receive error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		// Load user-specific environment.
		context.UserMaildir = maildir.Dir(fmt.Sprintf("%s%s/", worker.Config.Workers[worker.Name].MaildirRoot, context.UserName))
		context.UserCRDT = fmt.Sprintf("%s%s/", worker.Config.Workers[worker.Name].CRDTLayerRoot, context.UserName)

		switch {

		case rawReq == "> done <":
			// TODO: Trigger state-dependent behaviour when user logged out.
			log.Printf("%s: done.", context.UserName)

		case rawReq == "> changed <":
			// TODO: Trigger state-dependent behaviour when session changed.
			log.Printf("%s: session changed.", context.UserName)

		case req.Command == "SELECT":
			if ok := worker.Select(c, req, context); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			err = c.SignalSessionDone(nil)
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}
		}

		// Receive next incoming client command.
		rawReq, err = c.Receive()
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}
	}

	log.Println("DISTRIBUTOR sent '> done <'")
}
