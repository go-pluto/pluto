package imap

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/comm"
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
func InitStorage(config *config.Config) (*Storage, *comm.Receiver, error) {

	var err error

	// Initialize and set fields.
	storage := &Storage{
		Config: config,
	}

	// TODO: For all users known to this node via the authentication mechanism,
	//       read in their respective CRDT mailbox states from stable storage and
	//       place them in a map accessible via user name as key.

	// Load internal TLS config.
	internalTLSConfig, err := crypto.NewInternalTLSConfig(config.Storage.TLS.CertLoc, config.Storage.TLS.KeyLoc, config.RootCertLoc)
	if err != nil {
		return nil, nil, err
	}

	// Start to listen for incoming internal connections on defined IP and sync port.
	storage.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", config.Storage.IP, config.Storage.SyncPort), internalTLSConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("[imap.InitStorage] Listening for internal TLS connections failed with: %s\n", err.Error())
	}

	// Initialize receiving goroutine for sync operations.
	// TODO: Storage has to iterate over all worker nodes it is serving
	//       as CRDT backend for and create a 'CRDT-subnet' for each.
	recv, _, _, err := comm.InitReceiverForeground("storage", filepath.Join(config.Storage.CRDTLayerRoot, "receiving.log"), storage.Socket, []string{"worker-1"})
	if err != nil {
		return nil, nil, err
	}

	log.Printf("[imap.InitStorage] Listening for incoming sync requests on %s.\n", storage.Socket.Addr())

	return storage, recv, nil
}
