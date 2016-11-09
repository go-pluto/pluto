package imap

import (
	"fmt"
	"log"
	"net"
	"os"

	"crypto/tls"

	"github.com/numbleroot/pluto/auth"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Constants

// Integer counter for defining node types.
const (
	DISTRIBUTOR Type = iota
	WORKER
	STORAGE
)

// Structs

// Type declares what role a node takes in the system.
type Type int

// Node struct bundles information of one node instance.
type Node struct {
	Type        Type
	Name        string
	Socket      net.Listener
	AuthAdapter auth.PlainAuthenticator
	Connections map[string]*tls.Conn
	Config      *config.Config
}

// Functions

// InitNode listens for TLS connections on a TCP socket
// opened up on supplied IP address and port. It returns
// those information bundeled in above Node struct.
func InitNode(config *config.Config, distributor bool, worker string, storage bool) (*Node, error) {

	var err error

	node := new(Node)

	// Check if no type indicator was supplied, not possible.
	if !distributor && worker == "" && !storage {
		return nil, fmt.Errorf("[imap.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n")
	}

	// Check if multiple type indicators were supplied, not possible.
	if (distributor && worker != "" && storage) || (distributor && worker != "") || (distributor && storage) || (worker != "" && storage) {
		return nil, fmt.Errorf("[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n")
	}

	if distributor {

		// Set struct type to distributor.
		node.Type = DISTRIBUTOR
		node.Name = "distributor"
		node.Connections = make(map[string]*tls.Conn)

		// As the distributor is responsible for the authentication
		// of incoming requests, connect to provided auth mechanism.
		if config.Distributor.AuthAdapter == "AuthFile" {

			// Open authentication file and read user information.
			node.AuthAdapter, err = auth.NewFileAuthenticator(config.Distributor.AuthFile.File, config.Distributor.AuthFile.Separator)
			if err != nil {
				return nil, err
			}
		} else if config.Distributor.AuthAdapter == "AuthPostgreSQL" {

			// Connect to a PostgreSQL database for authentication measures.
			node.AuthAdapter, err = auth.NewPostgreSQLAuthenticator(config.Distributor.AuthPostgreSQL.IP, config.Distributor.AuthPostgreSQL.Port, config.Distributor.AuthPostgreSQL.Database, config.Distributor.AuthPostgreSQL.User, os.Getenv("IMAP_AUTH_POSTGRES_DATABASE_PASSWORD"), config.Distributor.AuthPostgreSQL.SSLMode)
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

			// Try to connect to worker node with internal TLS config.
			c, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", worker.IP, worker.Port), internalTLSConfig)
			if err != nil {
				return nil, fmt.Errorf("[imap.InitNode] Node %s could not connect to %s because of: %s\n", node.Type.String(), name, err.Error())
			}

			// Save connection for later use.
			node.Connections[name] = c
		}

		// Load public TLS config based on config values.
		publicTLSConfig, err := crypto.NewPublicTLSConfig(config.Distributor.PublicTLS.CertLoc, config.Distributor.PublicTLS.KeyLoc)
		if err != nil {
			return nil, err
		}

		// Start to listen for incoming public connections on defined IP and port.
		node.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Distributor.IP, config.Distributor.Port), publicTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitNode] Listening for public TLS connections as %s failed with: %s\n", node.Type.String(), err.Error())
		}

		log.Printf("[imap.InitNode] Listening as %s node for incoming IMAP requests on %s.\n", node.Type.String(), node.Socket.Addr())

	} else if worker != "" {

		// Check if supplied worker ID actually is configured.
		if _, ok := config.Workers[worker]; !ok {

			var workerID string

			// Retrieve first valid worker ID to provide feedback.
			for workerID = range config.Workers {
				break
			}

			return nil, fmt.Errorf("[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example '%s'.\n", workerID)
		}

		// Set struct type to worker.
		node.Type = WORKER
		node.Name = worker
		node.Connections = make(map[string]*tls.Conn)

		// Load internal TLS config.
		internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Workers[worker].TLS.CertLoc, config.Workers[worker].TLS.KeyLoc, config.RootCertLoc)
		if err != nil {
			return nil, err
		}

		// Try to connect to storage node with internal TLS config.
		c, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.Port), internalTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitNode] Node %s could not connect to STORAGE because of: %s\n", node.Type.String(), err.Error())
		}

		// Save connection for later use.
		node.Connections["storage"] = c

		// Start to listen for incoming internal connections on defined IP and port.
		node.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Workers[worker].IP, config.Workers[worker].Port), internalTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitNode] Listening for public TLS connections as %s failed with: %s\n", node.Type.String(), err.Error())
		}

		log.Printf("[imap.InitNode] Listening as %s node for incoming IMAP requests on %s.\n", node.Type.String(), node.Socket.Addr())

	} else if storage {

		// Set struct type to storage.
		node.Type = STORAGE
		node.Name = "storage"

		// Load internal TLS config.
		internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Storage.TLS.CertLoc, config.Storage.TLS.KeyLoc, config.RootCertLoc)
		if err != nil {
			return nil, err
		}

		// Start to listen for incoming internal connections on defined IP and port.
		node.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.Port), internalTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitNode] Listening for public TLS connections as %s failed with: %s\n", node.Type.String(), err.Error())
		}

		log.Printf("[imap.InitNode] Listening as %s node for incoming IMAP requests on %s.\n", node.Type.String(), node.Socket.Addr())

	}

	// Set remaining general elements.
	node.Config = config

	return node, nil
}

// HandleRequest acts as the jump start for any new incoming
// connection to this node in a pluto system. It creates the
// needed connection structure and if appropriate to its type
// sends out an IMAP greeting. After that hand-off to the IMAP
// state machine is performed.
func (node *Node) HandleRequest(conn net.Conn) {

	// Create a new connection struct for incoming request.
	c := NewConnection(conn)

	if node.Type == DISTRIBUTOR {

		// Initially, each connection's state at distributor stage
		// is set to not authenticated as it was just started.
		c.IMAPState = NOT_AUTHENTICATED

		// If this node is a distributor, send initial server greeting.
		err := c.Send("* OK IMAP4rev1 " + node.Config.Distributor.IMAP.Greeting)
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		// Dispatch to distributor state.
		node.AcceptDistributor(c)

	} else if node.Type == WORKER {

		// Initially, each connection's state at worker stage
		// is set to authenticated as auth was performed at distributor.
		c.IMAPState = AUTHENTICATED

		// Dispatch to worker state.
		node.AcceptWorker(c)

	} else if node.Type == STORAGE {

		// The storage node only acts as a storage
		// location for replicas.
		// Dispatch to own state.
		node.AcceptStorage(c)
	}
}

// RunNode loops over incoming requests and
// dispatches each one to a goroutine taking
// care of the commands supplied.
func (node *Node) RunNode() error {

	for {

		// Accept request or fail on error.
		conn, err := node.Socket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.RunNode] Accepting incoming request failed with: %s\n", err.Error())
		}

		// Dispatch to goroutine.
		go node.HandleRequest(conn)
	}
}
