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
func InitNode(config *config.Config, distributor bool, worker string, storage bool) *Node {

	var err error
	node := new(Node)

	// Place arguments in corresponding struct members.
	node.IP = config.Distributor.IP
	node.Port = config.Distributor.Port

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
		log.Fatalf("[node.InitNode] Failed to load TLS cert and key: %s\n", err.Error())
	}

	// Build Common Name (CN) and Subject Alternate
	// Name (SAN) from tlsConfig.Certificates.
	tlsConfig.BuildNameToCertificate()

	// Start to listen on defined IP and port.
	node.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", node.IP, node.Port), tlsConfig)
	if err != nil {
		log.Fatalf("[node.InitNode] Listening for TLS connections on port failed with: %s\n", err.Error())
	}

	log.Printf("[node.InitNode] Listening for incoming IMAP requests on %s.\n", node.Socket.Addr())

	return node
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

		// If send returned a problem, the connection seems to be broken.
		// Log error and terminate this connection.
		log.Printf("[node.HandleRequest] Request terminated due to received Send error: %s\n", err.Error())

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
