package imap

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Structs

// Storage struct bundles information needed in
// operation of a storage node.
type Storage struct {
	Socket net.Listener
	Config *config.Config
}

// Functions

// InitStorage listens for TLS connections on a TCP socket
// opened up on supplied IP address. It returns those
// information bundeled in above Storage struct.
func InitStorage(config *config.Config) (*Storage, error) {

	var err error

	// Initialize and set fields.
	storage := &Storage{
		Config: config,
	}

	// Load internal TLS config.
	internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Storage.TLS.CertLoc, config.Storage.TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming internal connections on defined IP and sync port.
	storage.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.SyncPort), internalTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("[imap.InitStorage] Listening for internal TLS connections failed with: %s\n", err.Error())
	}

	log.Printf("[imap.InitStorage] Listening for incoming sync requests on %s.\n", storage.Socket.Addr())

	return storage, nil
}

// Run loops over incoming requests at storage and
// dispatches each one to a goroutine taking care of
// the commands supplied.
func (storage *Storage) Run() error {

	// TODO: Let the CRDT receiver function do the work.

	for {

		// Accept request or fail on error.
		conn, err := storage.Socket.Accept()
		if err != nil {
			return fmt.Errorf("[imap.Run] Accepting incoming request at storage failed with: %s\n", err.Error())
		}

		// Dispatch into own goroutine.
		go storage.HandleConnection(conn)
	}
}

// HandleConnection acts as the main loop for
// requests targeted at storage node.
func (storage *Storage) HandleConnection(conn net.Conn) {

	// Create a new connection struct for incoming request.
	c := NewConnection(conn)

	// Receive next incoming client command.
	inc, err := c.Receive()
	if err != nil {
		c.Error("Encountered receive error", err)
		return
	}

	log.Println(inc)
}
