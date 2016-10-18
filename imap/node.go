package imap

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
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
	Type   Type
	Config *config.Config
	Socket net.Listener
}

// Functions

// InitNode listens for TLS connections on a TCP socket
// opened up on supplied IP address and port. It returns
// those information bundeled in above Node struct.
func InitNode(config *config.Config, distributor bool, worker string, storage bool) (*Node, error) {

	var err error
	var certPath string
	var keyPath string
	var ip string
	var port string

	node := new(Node)

	// Check if no type indicator was supplied, not possible.
	if !distributor && worker == "" && !storage {
		return nil, fmt.Errorf("[imap.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n")
	}

	// Check if multiple type indicators were supplied, not possible.
	if (distributor && worker != "" && storage) || (distributor && worker != "") || (distributor && storage) || (worker != "" && storage) {
		return nil, fmt.Errorf("[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n")
	}

	// TLS config is taken from the excellent blog post
	// "Achieving a Perfect SSL Labs Score with Go":
	// https://blog.bracelab.com/achieving-perfect-ssl-labs-score-with-go
	tlsConfig := &tls.Config{
		Certificates:             make([]tls.Certificate, 1),
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	if distributor {

		// Set struct type to distributor.
		node.Type = DISTRIBUTOR

		// Set config values to type specific values.
		certPath = config.Distributor.TLS.CertLoc
		keyPath = config.Distributor.TLS.KeyLoc
		ip = config.Distributor.IP
		port = config.Distributor.Port

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

		// Set config values to type specific values.
		certPath = config.Workers[worker].TLS.CertLoc
		keyPath = config.Workers[worker].TLS.KeyLoc
		ip = config.Workers[worker].IP
		port = config.Workers[worker].Port

	} else if storage {

		// Set struct type to storage.
		node.Type = STORAGE

		// Set config values to type specific values.
		certPath = config.Storage.TLS.CertLoc
		keyPath = config.Storage.TLS.KeyLoc
		ip = config.Storage.IP
		port = config.Storage.Port

	}

	// Put in supplied TLS cert and key.
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitNode] Failed to load %s TLS cert and key: %s\n", node.Type.String(), err.Error())
	}

	// Build Common Name (CN) and Subject Alternate
	// Name (SAN) from tlsConfig.Certificates.
	tlsConfig.BuildNameToCertificate()

	// Start to listen on defined IP and port.
	node.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", ip, port), tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitNode] Listening for TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitNode] Listening as %s node for incoming IMAP requests on %s.\n", node.Type.String(), node.Socket.Addr())

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

		// If this node is a distributor, send initial server greeting.
		err := c.Send("* OK IMAP4rev1 " + node.Config.Distributor.IMAP.Greeting)
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		// Dispatch to not-authenticated state.
		c.Transition(node, NOT_AUTHENTICATED)

	} else if node.Type == WORKER {

		// Connections to IMAP worker nodes contain
		// already authenticated requests.
		// Dispatch to authenticated state.
		c.Transition(node, AUTHENTICATED)

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
