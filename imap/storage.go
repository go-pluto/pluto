package imap

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
	"github.com/numbleroot/pluto/crypto"
)

// Structs

// Storage struct bundles information needed in
// operation of a storage node.
type Storage struct {
	lock             *sync.RWMutex
	Socket           net.Listener
	MailboxStructure map[string]map[string]*crdt.ORSet
	MailboxContents  map[string]map[string][]string
	ApplyCRDTUpdChan chan string
	DoneCRDTUpdChan  chan struct{}
	Config           *config.Config
}

// Functions

// InitStorage listens for TLS connections on a TCP socket
// opened up on supplied IP address. It returns those
// information bundeled in above Storage struct.
func InitStorage(config *config.Config) (*Storage, error) {

	var err error

	// Initialize and set fields.
	storage := &Storage{
		lock:             new(sync.RWMutex),
		MailboxStructure: make(map[string]map[string]*crdt.ORSet),
		MailboxContents:  make(map[string]map[string][]string),
		ApplyCRDTUpdChan: make(chan string),
		DoneCRDTUpdChan:  make(chan struct{}),
		Config:           config,
	}

	// Find all files below this node's CRDT root layer.
	userFolders, err := filepath.Glob(filepath.Join(config.Storage.CRDTLayerRoot, "*"))
	if err != nil {
		return nil, fmt.Errorf("[imap.InitStorage] Globbing for CRDT folders of users failed with: %s\n", err.Error())
	}

	for _, folder := range userFolders {

		// Retrieve information about accessed file.
		folderInfo, err := os.Stat(folder)
		if err != nil {
			return nil, fmt.Errorf("[imap.InitStorage] Error during stat'ing possible user CRDT folder: %s\n", err.Error())
		}

		// Only consider folders for building up CRDT map.
		if folderInfo.IsDir() {

			// Extract last part of path, the user name.
			userName := filepath.Base(folder)

			// Read in mailbox structure CRDT from file.
			userMainCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, "mailbox-structure.log"))
			if err != nil {
				return nil, fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %s\n", err.Error())
			}

			// Store main CRDT in designated map for user name.
			storage.MailboxStructure[userName] = make(map[string]*crdt.ORSet)
			storage.MailboxStructure[userName]["Structure"] = userMainCRDT

			// Already initialize slice to track order in mailbox.
			storage.MailboxContents[userName] = make(map[string][]string)

			// Retrieve all mailboxes the user possesses
			// according to main CRDT.
			userMailboxes := userMainCRDT.GetAllValues()

			for _, userMailbox := range userMailboxes {

				// Read in each mailbox CRDT from file.
				userMailboxCRDT, err := crdt.InitORSetFromFile(filepath.Join(folder, fmt.Sprintf("%s.log", userMailbox)))
				if err != nil {
					return nil, fmt.Errorf("[imap.InitStorage] Reading CRDT failed: %s\n", err.Error())
				}

				// Store each read-in CRDT in map under the respective
				// mailbox name in user's main CRDT.
				storage.MailboxStructure[userName][userMailbox] = userMailboxCRDT

				// Read in mails in respective mailbox in order to
				// allow sequence numbers actions.
				storage.MailboxContents[userName][userMailbox] = userMailboxCRDT.GetAllValues()
			}
		}
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

	// Initialize receiving goroutine for sync operations.
	// TODO: Storage has to iterate over all worker nodes it is serving
	//       as CRDT backend for and create a 'CRDT-subnet' for each.
	_, _, err = comm.InitReceiver("storage", filepath.Join(config.Storage.CRDTLayerRoot, "receiving.log"), storage.Socket, storage.ApplyCRDTUpdChan, storage.DoneCRDTUpdChan, []string{"worker-1"})
	if err != nil {
		return nil, err
	}

	log.Printf("[imap.InitStorage] Listening for incoming sync requests on %s.\n", storage.Socket.Addr())

	return storage, nil
}

// ApplyCRDTUpd receives strings representing CRDT
// update operations from receiver and executes them.
func (storage *Storage) ApplyCRDTUpd() error {

	for {

		// Receive update message from receiver
		// via channel.
		updMsg := <-storage.ApplyCRDTUpdChan

		// Parse operation that payload specifies.
		op, opPayload, err := comm.ParseOp(updMsg)
		if err != nil {
			return fmt.Errorf("[imap.ApplyCRDTUpd] Error while parsing operation from sync message: %s\n", err.Error())
		}

		// Depending on received operation,
		// parse remaining payload further.
		switch op {

		case "create":

			// Parse received payload message into create message struct.
			createUpd, err := comm.ParseCreate(opPayload)
			if err != nil {
				return fmt.Errorf("[imap.ApplyCRDTUpd] Error while parsing CREATE update from sync message: %s\n", err.Error())
			}

			// Lock storage exclusively.
			storage.lock.Lock()

			// Save user's mailbox structure CRDT to more
			// conveniently use it hereafter.
			userMainCRDT := storage.MailboxStructure[createUpd.User]["Structure"]

			// Create a new Maildir on stable storage.
			posMaildir := maildir.Dir(filepath.Join(storage.Config.Storage.MaildirRoot, createUpd.User, createUpd.Mailbox))

			err = posMaildir.Create()
			if err != nil {
				storage.lock.Unlock()
				return fmt.Errorf("[imap.ApplyCRDTUpd] Maildir for new mailbox could not be created: %s\n", err.Error())
			}

			// Construct path to new CRDT file.
			posMailboxCRDTPath := filepath.Join(storage.Config.Storage.CRDTLayerRoot, createUpd.User, fmt.Sprintf("%s.log", createUpd.Mailbox))

			// Initialize new ORSet for new mailbox.
			posMailboxCRDT, err := crdt.InitORSetWithFile(posMailboxCRDTPath)
			if err != nil {

				// Perform clean up.
				log.Printf("[imap.ApplyCRDTUpd] CREATE fail: %s\n", err.Error())
				log.Printf("[imap.ApplyCRDTUpd] Removing just created Maildir completely...\n")

				// Attempt to remove Maildir.
				err = posMaildir.Remove()
				if err != nil {
					log.Printf("[imap.ApplyCRDTUpd] ... failed to remove Maildir: %s\n", err.Error())
					log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
				} else {
					log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
				}

				// Exit worker.
				storage.lock.Unlock()
				os.Exit(1)
			}

			// Place newly created CRDT in mailbox structure.
			storage.MailboxStructure[createUpd.User][createUpd.Mailbox] = posMailboxCRDT

			// Initialize contents slice for new mailbox to track
			// message sequence numbers in it.
			storage.MailboxContents[createUpd.User][createUpd.Mailbox] = make([]string, 0, 6)

			// If succeeded, add a new folder in user's main CRDT.
			err = userMainCRDT.AddEffect(createUpd.AddMailbox.Value, createUpd.AddMailbox.Tag, true, true)
			if err != nil {

				// Perform clean up.
				log.Printf("[imap.ApplyCRDTUpd] CREATE fail: %s\n", err.Error())
				log.Printf("[imap.Create] Removing added CRDT from mailbox structure and contents slice...\n")

				// Remove just added CRDT of new maildir from mailbox structure
				// and corresponding contents slice.
				delete(storage.MailboxStructure[createUpd.User], createUpd.Mailbox)
				delete(storage.MailboxContents[createUpd.User], createUpd.Mailbox)

				log.Printf("[imap.Create] ... done. Removing just created Maildir completely...\n")

				// Attempt to remove Maildir.
				err = posMaildir.Remove()
				if err != nil {
					log.Printf("[imap.ApplyCRDTUpd] ... failed to remove Maildir: %s\n", err.Error())
					log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
				} else {
					log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
				}

				// Exit worker.
				storage.lock.Unlock()
				os.Exit(1)
			}

			// Unlock storage.
			storage.lock.Unlock()

		case "delete":

			// Parse received payload message into delete message struct.
			deleteUpd, err := comm.ParseDelete(opPayload)
			if err != nil {
				return fmt.Errorf("[imap.ApplyCRDTUpd] Error while parsing DELETE update from sync message: %s\n", err.Error())
			}

			// Lock storage exclusively.
			storage.lock.Lock()

			// Save user's mailbox structure CRDT to more
			// conveniently use it hereafter.
			userMainCRDT := storage.MailboxStructure[deleteUpd.User]["Structure"]

			// Construct remove set from received values.
			rSet := make(map[string]string)
			for _, element := range deleteUpd.RmvMailbox {
				rSet[element.Tag] = element.Value
			}

			// Remove received pairs from user's main CRDT.
			err = userMainCRDT.RemoveEffect(rSet, true, true)
			if err != nil {
				storage.lock.Unlock()
				return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to remove elements from user's main CRDT: %s\n", err.Error())
			}

			// Remove CRDT from mailbox structure and corresponding
			// mail contents slice.
			delete(storage.MailboxStructure[deleteUpd.User], deleteUpd.Mailbox)
			delete(storage.MailboxContents[deleteUpd.User], deleteUpd.Mailbox)

			// Construct path to CRDT file to delete.
			delMailboxCRDTPath := filepath.Join(storage.Config.Storage.CRDTLayerRoot, deleteUpd.User, fmt.Sprintf("%s.log", deleteUpd.Mailbox))

			// Remove CRDT file of mailbox.
			err = os.Remove(delMailboxCRDTPath)
			if err != nil {
				storage.lock.Unlock()
				return fmt.Errorf("[imap.ApplyCRDTUpd] CRDT file of mailbox could not be deleted: %s\n", err.Error())
			}

			// Remove files associated with deleted mailbox
			// from stable storage.
			delMaildir := maildir.Dir(filepath.Join(storage.Config.Storage.MaildirRoot, deleteUpd.User, deleteUpd.Mailbox))

			err = delMaildir.Remove()
			if err != nil {
				storage.lock.Unlock()
				return fmt.Errorf("[imap.ApplyCRDTUpd] Maildir could not be deleted: %s\n", err.Error())
			}

			// Unlock storage.
			storage.lock.Unlock()

		case "append":

			// Parse received payload message into append message struct.
			appendUpd, err := comm.ParseAppend(opPayload)
			if err != nil {
				return fmt.Errorf("[imap.ApplyCRDTUpd] Error while parsing APPEND update from sync message: %s\n", err.Error())
			}

			// Lock storage exclusively.
			storage.lock.Lock()

			// Save user's mailbox structure CRDT to more
			// conveniently use it hereafter.
			userMainCRDT := storage.MailboxStructure[appendUpd.User]["Structure"]

			// Check if specified mailbox from append message is present
			// in user's main CRDT on this node.
			if userMainCRDT.Lookup(appendUpd.Mailbox, true) {

				// Store concerned mailbox CRDT.
				userMailboxCRDT := storage.MailboxStructure[appendUpd.User][appendUpd.Mailbox]

				// Check if mail is not yet present on this node.
				if userMailboxCRDT.Lookup(appendUpd.AddMail.Value, true) != true {

					// Construct path to new file.
					var appendFileName string
					if appendUpd.Mailbox == "INBOX" {
						appendFileName = filepath.Join(storage.Config.Storage.MaildirRoot, appendUpd.User, "cur", appendUpd.AddMail.Value)
					} else {
						appendFileName = filepath.Join(storage.Config.Storage.MaildirRoot, appendUpd.User, appendUpd.Mailbox, "cur", appendUpd.AddMail.Value)
					}

					// If so, place file contents at correct location.
					appendFile, err := os.Create(appendFileName)
					if err != nil {
						storage.lock.Unlock()
						return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to create file for mail to append: %s\n", err.Error())
					}

					_, err = appendFile.WriteString(appendUpd.AddMail.Contents)
					if err != nil {

						// Perform clean up.
						log.Printf("[imap.ApplyCRDTUpd] APPEND fail: %s\n", err.Error())
						log.Printf("[imap.ApplyCRDTUpd] Removing just created mail file...\n")

						// Remove just created mail file.
						err = os.Remove(appendFileName)
						if err != nil {
							log.Printf("[imap.ApplyCRDTUpd] ... failed: %s\n", err.Error())
							log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
						} else {
							log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
						}

						// Exit worker.
						storage.lock.Unlock()
						os.Exit(1)
					}

					// Sync contents to stable storage.
					err = appendFile.Sync()
					if err != nil {

						// Perform clean up.
						log.Printf("[imap.ApplyCRDTUpd] APPEND fail: %s\n", err.Error())
						log.Printf("[imap.ApplyCRDTUpd] Removing just created mail file...\n")

						// Remove just created mail file.
						err = os.Remove(appendFileName)
						if err != nil {
							log.Printf("[imap.ApplyCRDTUpd] ... failed: %s\n", err.Error())
							log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
						} else {
							log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
						}

						// Exit worker.
						storage.lock.Unlock()
						os.Exit(1)
					}

					// Append new mail to mailbox' contents CRDT.
					storage.MailboxContents[appendUpd.User][appendUpd.Mailbox] = append(storage.MailboxContents[appendUpd.User][appendUpd.Mailbox], appendUpd.AddMail.Value)

					// If succeeded, add new mail to mailbox' CRDT.
					err = userMailboxCRDT.AddEffect(appendUpd.AddMail.Value, appendUpd.AddMail.Tag, true, true)
					if err != nil {

						// Perform clean up.
						log.Printf("[imap.ApplyCRDTUpd] APPEND fail: %s\n", err.Error())
						log.Printf("[imap.ApplyCRDTUpd] Removing just created mail file...\n")

						// Remove just created mail file.
						err = os.Remove(appendFileName)
						if err != nil {
							log.Printf("[imap.ApplyCRDTUpd] ... failed: %s\n", err.Error())
							log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
						} else {
							log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
						}

						// Exit worker.
						storage.lock.Unlock()
						os.Exit(1)
					}
				} else {

					// Add new mail to mailbox' CRDT.
					err = userMailboxCRDT.AddEffect(appendUpd.AddMail.Value, appendUpd.AddMail.Tag, true, true)
					if err != nil {
						storage.lock.Unlock()
						return fmt.Errorf("[imap.ApplyCRDTUpd] APPEND fail: %s. Exiting.\n", err.Error())
					}
				}
			}

			// Unlock storage.
			storage.lock.Unlock()

		case "expunge":

			// Parse received payload message into expunge message struct.
			expungeUpd, err := comm.ParseExpunge(opPayload)
			if err != nil {
				return fmt.Errorf("[imap.ApplyCRDTUpd] Error while parsing EXPUNGE update from sync message: %s\n", err.Error())
			}

			// Lock storage exclusively.
			storage.lock.Lock()

			// Save user's mailbox structure CRDT to more
			// conveniently use it hereafter.
			userMainCRDT := storage.MailboxStructure[expungeUpd.User]["Structure"]

			// Check if specified mailbox from expunge message is
			// present in user's main CRDT on this node.
			if userMainCRDT.Lookup(expungeUpd.Mailbox, true) {

				// Store concerned mailbox CRDT.
				userMailboxCRDT := storage.MailboxStructure[expungeUpd.User][expungeUpd.Mailbox]

				// Construct remove set from received values.
				rSet := make(map[string]string)
				for _, element := range expungeUpd.RmvMail {
					rSet[element.Tag] = element.Value
				}

				// Delete supplied elements from mailbox.
				err := userMailboxCRDT.RemoveEffect(rSet, true, true)
				if err != nil {
					storage.lock.Unlock()
					return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to remove mail elements from respective mailbox CRDT: %s\n", err.Error())
				}

				// Check if just removed elements marked all
				// instances of mail file.
				if userMailboxCRDT.Lookup(expungeUpd.RmvMail[0].Value, true) != true {

					// Construct path to old file.
					var delFileName string
					if expungeUpd.Mailbox == "INBOX" {
						delFileName = filepath.Join(storage.Config.Storage.MaildirRoot, expungeUpd.User, "cur", expungeUpd.RmvMail[0].Value)
					} else {
						delFileName = filepath.Join(storage.Config.Storage.MaildirRoot, expungeUpd.User, expungeUpd.Mailbox, "cur", expungeUpd.RmvMail[0].Value)
					}

					// Remove the file.
					err := os.Remove(delFileName)
					if err != nil {
						storage.lock.Unlock()
						return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to remove underlying mail file during EXPUNGE update: %s\n", err.Error())
					}
				}

				for msgNum, msgName := range storage.MailboxContents[expungeUpd.User][expungeUpd.Mailbox] {

					// Find removed mail file's sequence number.
					if msgName == expungeUpd.RmvMail[0].Value {

						// Delete mail's sequence number from contents structure.
						realMsgNum := msgNum + 1
						storage.MailboxContents[expungeUpd.User][expungeUpd.Mailbox] = append(storage.MailboxContents[expungeUpd.User][expungeUpd.Mailbox][:msgNum], storage.MailboxContents[expungeUpd.User][expungeUpd.Mailbox][realMsgNum:]...)
					}
				}
			}

			// Unlock storage.
			storage.lock.Unlock()

		case "store":

			// Parse received payload message into store message struct.
			storeUpd, err := comm.ParseStore(opPayload)
			if err != nil {
				return fmt.Errorf("[imap.ApplyCRDTUpd] Error while parsing STORE update from sync message: %s\n", err.Error())
			}

			// Lock storage exclusively.
			storage.lock.Lock()

			// Save user's mailbox structure CRDT to more
			// conveniently use it hereafter.
			userMainCRDT := storage.MailboxStructure[storeUpd.User]["Structure"]

			// Check if specified mailbox from store message is present
			// in user's main CRDT on this node.
			if userMainCRDT.Lookup(storeUpd.Mailbox, true) {

				// Store concerned mailbox CRDT.
				userMailboxCRDT := storage.MailboxStructure[storeUpd.User][storeUpd.Mailbox]

				// Construct remove set from received values.
				rSet := make(map[string]string)
				for _, element := range storeUpd.RmvMail {
					rSet[element.Tag] = element.Value
				}

				// Delete supplied elements from mailbox.
				err := userMailboxCRDT.RemoveEffect(rSet, true, true)
				if err != nil {
					storage.lock.Unlock()
					return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to remove mail elements from respective mailbox CRDT: %s\n", err.Error())
				}

				// Check if just removed elements marked all
				// instances of mail file.
				if userMailboxCRDT.Lookup(storeUpd.RmvMail[0].Value, true) != true {

					// Construct path to old file.
					var delFileName string
					if storeUpd.Mailbox == "INBOX" {
						delFileName = filepath.Join(storage.Config.Storage.MaildirRoot, storeUpd.User, "cur", storeUpd.RmvMail[0].Value)
					} else {
						delFileName = filepath.Join(storage.Config.Storage.MaildirRoot, storeUpd.User, storeUpd.Mailbox, "cur", storeUpd.RmvMail[0].Value)
					}

					// Remove the file.
					err := os.Remove(delFileName)
					if err != nil {
						storage.lock.Unlock()
						return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to remove underlying mail file during STORE update: %s\n", err.Error())
					}
				}

				// Check if new mail name is not yet present
				// on this node.
				if userMailboxCRDT.Lookup(storeUpd.AddMail.Value, true) != true {

					// Construct path to new file.
					var storeFileName string
					if storeUpd.Mailbox == "INBOX" {
						storeFileName = filepath.Join(storage.Config.Storage.MaildirRoot, storeUpd.User, "cur", storeUpd.AddMail.Value)
					} else {
						storeFileName = filepath.Join(storage.Config.Storage.MaildirRoot, storeUpd.User, storeUpd.Mailbox, "cur", storeUpd.AddMail.Value)
					}

					// If not yet present on node, place file
					// contents at correct location.
					storeFile, err := os.Create(storeFileName)
					if err != nil {
						storage.lock.Unlock()
						return fmt.Errorf("[imap.ApplyCRDTUpd] Failed to create file for mail of STORE operation: %s\n", err.Error())
					}

					_, err = storeFile.WriteString(storeUpd.AddMail.Contents)
					if err != nil {

						// Perform clean up.
						log.Printf("[imap.ApplyCRDTUpd] STORE fail: %s\n", err.Error())
						log.Printf("[imap.ApplyCRDTUpd] Removing just created mail file...\n")

						// Remove just created mail file.
						err = os.Remove(storeFileName)
						if err != nil {
							log.Printf("[imap.ApplyCRDTUpd] ... failed: %s\n", err.Error())
							log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
						} else {
							log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
						}

						// Exit worker.
						storage.lock.Unlock()
						os.Exit(1)
					}

					// Sync contents to stable storage.
					err = storeFile.Sync()
					if err != nil {

						// Perform clean up.
						log.Printf("[imap.ApplyCRDTUpd] STORE fail: %s\n", err.Error())
						log.Printf("[imap.ApplyCRDTUpd] Removing just created mail file...\n")

						// Remove just created mail file.
						err = os.Remove(storeFileName)
						if err != nil {
							log.Printf("[imap.ApplyCRDTUpd] ... failed: %s\n", err.Error())
							log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
						} else {
							log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
						}

						// Exit worker.
						storage.lock.Unlock()
						os.Exit(1)
					}

					// If succeeded, add renamed mail to mailbox' CRDT.
					err = userMailboxCRDT.AddEffect(storeUpd.AddMail.Value, storeUpd.AddMail.Tag, true, true)
					if err != nil {

						// Perform clean up.
						log.Printf("[imap.ApplyCRDTUpd] STORE fail: %s\n", err.Error())
						log.Printf("[imap.ApplyCRDTUpd] Removing just created mail file...\n")

						// Remove just created mail file.
						err = os.Remove(storeFileName)
						if err != nil {
							log.Printf("[imap.ApplyCRDTUpd] ... failed: %s\n", err.Error())
							log.Printf("[imap.ApplyCRDTUpd] Exiting.\n")
						} else {
							log.Printf("[imap.ApplyCRDTUpd] ... done. Exiting.\n")
						}

						// Exit worker.
						storage.lock.Unlock()
						os.Exit(1)
					}
				} else {

					// Add renamed mail to mailbox' CRDT.
					err = userMailboxCRDT.AddEffect(storeUpd.AddMail.Value, storeUpd.AddMail.Tag, true, true)
					if err != nil {
						storage.lock.Unlock()
						return fmt.Errorf("[imap.ApplyCRDTUpd] STORE fail: %s. Exiting.\n", err.Error())
					}
				}

				for msgNum, msgName := range storage.MailboxContents[storeUpd.User][storeUpd.Mailbox] {

					// Find old mail file's sequence number.
					if msgName == storeUpd.RmvMail[0].Value {

						// Replace old file name with renamed new one.
						storage.MailboxContents[storeUpd.User][storeUpd.Mailbox][msgNum] = storeUpd.AddMail.Value
					}
				}
			}

			// Unlock storage.
			storage.lock.Unlock()

		}

		// Signal receiver that update was performed.
		storage.DoneCRDTUpdChan <- struct{}{}
	}
}
