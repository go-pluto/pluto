package node

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
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
	IP     string
	Port   string
	Socket net.Listener
}

// Functions

// InitNode listens for TLS connections on a TCP socket
// opened up on supplied IP address and port. It returns
// those information bundeled in above Node struct.
func InitNode(config *config.Config, distributor bool, worker string, storage bool) (*Node, error) {

	var err error
	node := new(Node)

	// Place arguments in corresponding struct members.
	node.IP = config.Distributor.IP
	node.Port = config.Distributor.Port

	// Check if no type indicator was supplied, not possible.
	if !distributor && worker == "" && !storage {
		return nil, fmt.Errorf("[node.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n")
	}

	// Check if multiple type indicators were supplied, not possible.
	if (distributor && worker != "" && storage) || (distributor && worker != "") || (distributor && storage) || (worker != "" && storage) {
		return nil, fmt.Errorf("[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n")
	}

	if distributor {
		// TODO: Continue working here.
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

	// Put in supplied TLS cert and key.
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(config.Distributor.TLS.CertLoc, config.Distributor.TLS.KeyLoc)
	if err != nil {
		return nil, fmt.Errorf("[node.InitNode] Failed to load TLS cert and key: %s\n", err.Error())
	}

	// Build Common Name (CN) and Subject Alternate
	// Name (SAN) from tlsConfig.Certificates.
	tlsConfig.BuildNameToCertificate()

	// Start to listen on defined IP and port.
	node.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", node.IP, node.Port), tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("[node.InitNode] Listening for TLS connections on port failed with: %s\n", err.Error())
	}

	log.Printf("[node.InitNode] Listening for incoming IMAP requests on %s.\n", node.Socket.Addr())

	return node, nil
}

// HandleRequest acts as the jump start for any new
// incoming connection to pluto. It creates the needed
// control structure, sends out the initial server
// greeting and after that hands over control to the
// IMAP state machine.
func (node *Node) HandleRequest(conn net.Conn, greeting string) {

	// Create a new connection struct for incoming request.
	c := imap.NewConnection(conn)

	// Send initial server greeting.
	err := c.Send("* OK IMAP4rev1 " + greeting)
	if err != nil {
		c.Error("Encountered send error", err)
		return
	}

	// Dispatch to not-authenticated state.
	c.Transition(imap.NOT_AUTHENTICATED)
}

// RunNode loops over incoming requests and
// dispatches each one to a goroutine taking
// care of the commands supplied.
func (node *Node) RunNode(greeting string) {

	for {

		// Accept request or fail on error.
		conn, err := node.Socket.Accept()
		if err != nil {
			log.Fatalf("[node.RunNode] Accepting incoming request failed with: %s\n", err.Error())
		}

		// Dispatch to goroutine.
		go node.HandleRequest(conn, greeting)
	}
}
