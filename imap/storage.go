package imap

import (
	"fmt"
	"log"
	"net"
	"os"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/crypto"
)

// Structs

// Storage struct bundles information needed in
// operation of a storage node.
type Storage struct {
	Socket           net.Listener
	MailboxStructure map[string]map[string]*crdt.ORSet
	Config           *config.Config
}

// Functions

// InitStorage listens for TLS connections on a TCP socket
// opened up on supplied IP address. It returns those
// information bundeled in above Storage struct.
func InitStorage(config *config.Config) (*Storage, *comm.Receiver, error) {

	var err error

	// Initialize and set fields.
	storage := &Storage{
		MailboxStructure: make(map[string]map[string]*crdt.ORSet),
		Config:           config,
	}

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(config.Storage.CRDTLayerRoot, "*"))
	if err != nil {
		return nil, nil, fmt.Errorf("[imap.InitStorage] Globbing for CRDT folders of users failed with: %s\n", err.Error())
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return nil, nil, fmt.Errorf("[imap.InitStorage] Error during stat'ing possible user CRDT folder: %s\n", err.Error())
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetOpFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return nil, nil, fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %s\n", err.Error())
			}

			// Store main CRDT in designated map for user name.
			storage.MailboxStructure[userName] = make(map[string]*crdt.ORSet)
			storage.MailboxStructure[userName]["Structure"] = userMainCRDT

			// Retrieve all mailboxes the user possesses
			// according to main CRDT.
			userMailboxes := userMainCRDT.GetAllValues()

			for _, userMailbox := range userMailboxes {

				// Read in each mailbox CRDT from file.
				userMailboxCRDT, err := crdt.InitORSetOpFromFile(filepath.Join(folder, fmt.Sprintf("%s.log", userMailbox)))
				if err != nil {
					return nil, nil, fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %s\n", err.Error())
				}

				// Store each read-in CRDT in map under
				storage.MailboxStructure[userName][userMailbox] = userMailboxCRDT
			}
		}
	}

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
