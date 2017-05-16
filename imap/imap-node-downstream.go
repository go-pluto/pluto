package imap

import (
	"fmt"
	stdlog "log"
	"os"

	"path/filepath"

	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/crdt"
)

// ApplyCreate performs the downstream part
// of a CREATE operation.
func (node *IMAPNode) ApplyCreate(opPayload string) {

	// Parse received payload message into create message struct.
	createUpd, err := comm.ParseCreate(opPayload)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyCreate] Error while parsing CREATE update from sync message: %v", err)
	}

	// Lock node exclusively.
	node.lock.Lock()
	defer node.lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[createUpd.User]["Structure"]

	// Create a new Maildir on stable storage.
	posMaildir := maildir.Dir(filepath.Join(node.MaildirRoot, createUpd.User, createUpd.Mailbox))

	err = posMaildir.Create()
	if err != nil {
		stdlog.Fatalf("[imap.ApplyCreate] Maildir for new mailbox could not be created: %v", err)
	}

	// Construct path to new CRDT file.
	posMailboxCRDTPath := filepath.Join(node.CRDTLayerRoot, createUpd.User, fmt.Sprintf("%s.log", createUpd.Mailbox))

	// Initialize new ORSet for new mailbox.
	posMailboxCRDT, err := crdt.InitORSetWithFile(posMailboxCRDTPath)
	if err != nil {

		// Perform clean up.
		stdlog.Printf("[imap.ApplyCreate] CREATE fail: %v", err)
		stdlog.Printf("[imap.ApplyCreate] Removing just created Maildir completely...")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			stdlog.Printf("[imap.ApplyCreate] ... failed to remove Maildir: %v", err)
			stdlog.Printf("[imap.ApplyCreate] Exiting")
		} else {
			stdlog.Printf("[imap.ApplyCreate] ... done - exiting")
		}

		os.Exit(1)
	}

	// Place newly created CRDT in mailbox structure.
	node.MailboxStructure[createUpd.User][createUpd.Mailbox] = posMailboxCRDT

	// Initialize contents slice for new mailbox to track
	// message sequence numbers in it.
	node.MailboxContents[createUpd.User][createUpd.Mailbox] = make([]string, 0, 6)

	// If succeeded, add a new folder in user's main CRDT.
	err = userMainCRDT.AddEffect(createUpd.AddMailbox.Value, createUpd.AddMailbox.Tag, true)
	if err != nil {

		// Perform clean up.
		stdlog.Printf("[imap.ApplyCreate] CREATE fail: %v", err)
		stdlog.Printf("[imap.Create] Removing added CRDT from mailbox structure and contents slice...")

		// Remove just added CRDT of new maildir from mailbox structure
		// and corresponding contents slice.
		delete(node.MailboxStructure[createUpd.User], createUpd.Mailbox)
		delete(node.MailboxContents[createUpd.User], createUpd.Mailbox)

		stdlog.Printf("[imap.Create] ... done. Removing just created Maildir completely...")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			stdlog.Printf("[imap.ApplyCreate] ... failed to remove Maildir: %v", err)
			stdlog.Printf("[imap.ApplyCreate] Exiting")
		} else {
			stdlog.Printf("[imap.ApplyCreate] ... done - exiting")
		}

		os.Exit(1)
	}
}

// ApplyDelete performs the downstream part
// of a DELETE operation.
func (node *IMAPNode) ApplyDelete(opPayload string) {

	// Parse received payload message into delete message struct.
	deleteUpd, err := comm.ParseDelete(opPayload)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyDelete] Error while parsing DELETE update from sync message: %v", err)
	}

	// Construct remove set from received values.
	rmElements := make(map[string]string)
	for _, element := range deleteUpd.RmvMailbox {
		rmElements[element.Tag] = element.Value
	}

	// Lock node exclusively.
	node.lock.Lock()
	defer node.lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[deleteUpd.User]["Structure"]

	// Remove received pairs from user's main CRDT.
	err = userMainCRDT.RemoveEffect(rmElements, true)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyDelete] Failed to remove elements from user's main CRDT: %v", err)
	}

	// Remove CRDT from mailbox structure and corresponding
	// mail contents slice.
	delete(node.MailboxStructure[deleteUpd.User], deleteUpd.Mailbox)
	delete(node.MailboxContents[deleteUpd.User], deleteUpd.Mailbox)

	// Construct path to CRDT file to delete.
	delMailboxCRDTPath := filepath.Join(node.CRDTLayerRoot, deleteUpd.User, fmt.Sprintf("%s.log", deleteUpd.Mailbox))

	// Remove CRDT file of mailbox.
	err = os.Remove(delMailboxCRDTPath)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyDelete] CRDT file of mailbox could not be deleted: %v", err)
	}

	// Remove files associated with deleted mailbox
	// from stable storage.
	delMaildir := maildir.Dir(filepath.Join(node.MaildirRoot, deleteUpd.User, deleteUpd.Mailbox))

	err = delMaildir.Remove()
	if err != nil {
		stdlog.Fatalf("[imap.ApplyDelete] Maildir could not be deleted: %v", err)
	}
}

// ApplyAppend performs the downstream part
// of an APPEND operation.
func (node *IMAPNode) ApplyAppend(opPayload string) {

	// Parse received payload message into append message struct.
	appendUpd, err := comm.ParseAppend(opPayload)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyAppend] Error while parsing APPEND update from sync message: %v", err)
	}

	// Lock node exclusively.
	node.lock.Lock()
	defer node.lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[appendUpd.User]["Structure"]

	// Check if specified mailbox from append message is present
	// in user's main CRDT on this node.
	if userMainCRDT.Lookup(appendUpd.Mailbox) {

		// Store concerned mailbox CRDT.
		userMailboxCRDT := node.MailboxStructure[appendUpd.User][appendUpd.Mailbox]

		// Check if mail is not yet present on this node.
		if userMailboxCRDT.Lookup(appendUpd.AddMail.Value) != true {

			// Construct path to new file.
			var appendFileName string
			if appendUpd.Mailbox == "INBOX" {
				appendFileName = filepath.Join(node.MaildirRoot, appendUpd.User, "cur", appendUpd.AddMail.Value)
			} else {
				appendFileName = filepath.Join(node.MaildirRoot, appendUpd.User, appendUpd.Mailbox, "cur", appendUpd.AddMail.Value)
			}

			// If so, place file contents at correct location.
			appendFile, err := os.Create(appendFileName)
			if err != nil {
				stdlog.Fatalf("[imap.ApplyAppend] Failed to create file for mail to append: %v", err)
			}

			_, err = appendFile.WriteString(appendUpd.AddMail.Contents)
			if err != nil {

				// Perform clean up.
				stdlog.Printf("[imap.ApplyAppend] APPEND fail: %v", err)
				stdlog.Printf("[imap.ApplyAppend] Removing just created mail file...")

				// Remove just created mail file.
				err = os.Remove(appendFileName)
				if err != nil {
					stdlog.Printf("[imap.ApplyAppend] ... failed: %v", err)
					stdlog.Printf("[imap.ApplyAppend] Exiting")
				} else {
					stdlog.Printf("[imap.ApplyAppend] ... done - exiting")
				}

				os.Exit(1)
			}

			// Sync contents to stable storage.
			err = appendFile.Sync()
			if err != nil {

				// Perform clean up.
				stdlog.Printf("[imap.ApplyAppend] APPEND fail: %v", err)
				stdlog.Printf("[imap.ApplyAppend] Removing just created mail file...")

				// Remove just created mail file.
				err = os.Remove(appendFileName)
				if err != nil {
					stdlog.Printf("[imap.ApplyAppend] ... failed: %v", err)
					stdlog.Printf("[imap.ApplyAppend] Exiting")
				} else {
					stdlog.Printf("[imap.ApplyAppend] ... done - exiting")
				}

				os.Exit(1)
			}

			// Append new mail to mailbox' contents CRDT.
			node.MailboxContents[appendUpd.User][appendUpd.Mailbox] = append(node.MailboxContents[appendUpd.User][appendUpd.Mailbox], appendUpd.AddMail.Value)

			// If succeeded, add new mail to mailbox' CRDT.
			err = userMailboxCRDT.AddEffect(appendUpd.AddMail.Value, appendUpd.AddMail.Tag, true)
			if err != nil {

				// Perform clean up.
				stdlog.Printf("[imap.ApplyAppend] APPEND fail: %v", err)
				stdlog.Printf("[imap.ApplyAppend] Removing just created mail file...")

				// Remove just created mail file.
				err = os.Remove(appendFileName)
				if err != nil {
					stdlog.Printf("[imap.ApplyAppend] ... failed: %v", err)
					stdlog.Printf("[imap.ApplyAppend] Exiting")
				} else {
					stdlog.Printf("[imap.ApplyAppend] ... done - exiting")
				}

				os.Exit(1)
			}
		} else {

			// Add new mail to mailbox' CRDT.
			err = userMailboxCRDT.AddEffect(appendUpd.AddMail.Value, appendUpd.AddMail.Tag, true)
			if err != nil {
				stdlog.Fatalf("[imap.ApplyAppend] APPEND fail: %v", err)
			}
		}
	}
}

// ApplyExpunge performs the downstream part
// of an EXPUNGE operation.
func (node *IMAPNode) ApplyExpunge(opPayload string) {

	// Parse received payload message into expunge message struct.
	expungeUpd, err := comm.ParseExpunge(opPayload)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyExpunge] Error while parsing EXPUNGE update from sync message: %v", err)
	}

	// Lock node exclusively.
	node.lock.Lock()
	defer node.lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[expungeUpd.User]["Structure"]

	// Check if specified mailbox from expunge message is
	// present in user's main CRDT on this node.
	if userMainCRDT.Lookup(expungeUpd.Mailbox) {

		// Store concerned mailbox CRDT.
		userMailboxCRDT := node.MailboxStructure[expungeUpd.User][expungeUpd.Mailbox]

		// Construct remove set from received values.
		rmElements := make(map[string]string)
		for _, element := range expungeUpd.RmvMail {
			rmElements[element.Tag] = element.Value
		}

		// Delete supplied elements from mailbox.
		err := userMailboxCRDT.RemoveEffect(rmElements, true)
		if err != nil {
			stdlog.Fatalf("[imap.ApplyExpunge] Failed to remove mail elements from respective mailbox CRDT: %v", err)
		}

		// Check if just removed elements marked all
		// instances of mail file.
		if userMailboxCRDT.Lookup(expungeUpd.RmvMail[0].Value) != true {

			// Construct path to old file.
			var delFileName string
			if expungeUpd.Mailbox == "INBOX" {
				delFileName = filepath.Join(node.MaildirRoot, expungeUpd.User, "cur", expungeUpd.RmvMail[0].Value)
			} else {
				delFileName = filepath.Join(node.MaildirRoot, expungeUpd.User, expungeUpd.Mailbox, "cur", expungeUpd.RmvMail[0].Value)
			}

			// Remove the file.
			err := os.Remove(delFileName)
			if err != nil {
				stdlog.Fatalf("[imap.ApplyExpunge] Failed to remove underlying mail file during EXPUNGE update: %v", err)
			}
		}

		for msgNum, msgName := range node.MailboxContents[expungeUpd.User][expungeUpd.Mailbox] {

			// Find removed mail file's sequence number.
			if msgName == expungeUpd.RmvMail[0].Value {

				// Delete mail's sequence number from contents structure.
				realMsgNum := msgNum + 1
				node.MailboxContents[expungeUpd.User][expungeUpd.Mailbox] = append(node.MailboxContents[expungeUpd.User][expungeUpd.Mailbox][:msgNum], node.MailboxContents[expungeUpd.User][expungeUpd.Mailbox][realMsgNum:]...)
			}
		}
	}
}

// ApplyStore performs the downstream part
// of a STORE operation.
func (node *IMAPNode) ApplyStore(opPayload string) {

	// Parse received payload message into store message struct.
	storeUpd, err := comm.ParseStore(opPayload)
	if err != nil {
		stdlog.Fatalf("[imap.ApplyStore] Error while parsing STORE update from sync message: %v", err)
	}

	// Lock node exclusively.
	node.lock.Lock()
	defer node.lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[storeUpd.User]["Structure"]

	// Check if specified mailbox from store message is present
	// in user's main CRDT on this node.
	if userMainCRDT.Lookup(storeUpd.Mailbox) {

		// Store concerned mailbox CRDT.
		userMailboxCRDT := node.MailboxStructure[storeUpd.User][storeUpd.Mailbox]

		// Construct remove set from received values.
		rmElements := make(map[string]string)
		for _, element := range storeUpd.RmvMail {
			rmElements[element.Tag] = element.Value
		}

		// Delete supplied elements from mailbox.
		err := userMailboxCRDT.RemoveEffect(rmElements, true)
		if err != nil {
			stdlog.Fatalf("[imap.ApplyStore] Failed to remove mail elements from respective mailbox CRDT: %v", err)
		}

		// Check if just removed elements marked all
		// instances of mail file.
		if userMailboxCRDT.Lookup(storeUpd.RmvMail[0].Value) != true {

			// Construct path to old file.
			var delFileName string
			if storeUpd.Mailbox == "INBOX" {
				delFileName = filepath.Join(node.MaildirRoot, storeUpd.User, "cur", storeUpd.RmvMail[0].Value)
			} else {
				delFileName = filepath.Join(node.MaildirRoot, storeUpd.User, storeUpd.Mailbox, "cur", storeUpd.RmvMail[0].Value)
			}

			// Remove the file.
			err := os.Remove(delFileName)
			if err != nil {
				stdlog.Fatalf("[imap.ApplyStore] Failed to remove underlying mail file during STORE update: %v", err)
			}
		}

		// Check if new mail name is not yet present
		// on this node.
		if userMailboxCRDT.Lookup(storeUpd.AddMail.Value) != true {

			// Construct path to new file.
			var storeFileName string
			if storeUpd.Mailbox == "INBOX" {
				storeFileName = filepath.Join(node.MaildirRoot, storeUpd.User, "cur", storeUpd.AddMail.Value)
			} else {
				storeFileName = filepath.Join(node.MaildirRoot, storeUpd.User, storeUpd.Mailbox, "cur", storeUpd.AddMail.Value)
			}

			// If not yet present on node, place file
			// contents at correct location.
			storeFile, err := os.Create(storeFileName)
			if err != nil {
				stdlog.Fatalf("[imap.ApplyStore] Failed to create file for mail of STORE operation: %v", err)
			}

			_, err = storeFile.WriteString(storeUpd.AddMail.Contents)
			if err != nil {

				// Perform clean up.
				stdlog.Printf("[imap.ApplyStore] STORE fail: %v", err)
				stdlog.Printf("[imap.ApplyStore] Removing just created mail file...")

				// Remove just created mail file.
				err = os.Remove(storeFileName)
				if err != nil {
					stdlog.Printf("[imap.ApplyStore] ... failed: %v", err)
					stdlog.Printf("[imap.ApplyStore] Exiting")
				} else {
					stdlog.Printf("[imap.ApplyStore] ... done - exiting")
				}

				os.Exit(1)
			}

			// Sync contents to stable storage.
			err = storeFile.Sync()
			if err != nil {

				// Perform clean up.
				stdlog.Printf("[imap.ApplyStore] STORE fail: %v", err)
				stdlog.Printf("[imap.ApplyStore] Removing just created mail file...")

				// Remove just created mail file.
				err = os.Remove(storeFileName)
				if err != nil {
					stdlog.Printf("[imap.ApplyStore] ... failed: %v", err)
					stdlog.Printf("[imap.ApplyStore] Exiting")
				} else {
					stdlog.Printf("[imap.ApplyStore] ... done - exiting")
				}

				os.Exit(1)
			}

			// If succeeded, add renamed mail to mailbox' CRDT.
			err = userMailboxCRDT.AddEffect(storeUpd.AddMail.Value, storeUpd.AddMail.Tag, true)
			if err != nil {

				// Perform clean up.
				stdlog.Printf("[imap.ApplyStore] STORE fail: %v", err)
				stdlog.Printf("[imap.ApplyStore] Removing just created mail file...")

				// Remove just created mail file.
				err = os.Remove(storeFileName)
				if err != nil {
					stdlog.Printf("[imap.ApplyStore] ... failed: %v", err)
					stdlog.Printf("[imap.ApplyStore] Exiting")
				} else {
					stdlog.Printf("[imap.ApplyStore] ... done - exiting")
				}

				os.Exit(1)
			}
		} else {

			// Add renamed mail to mailbox' CRDT.
			err = userMailboxCRDT.AddEffect(storeUpd.AddMail.Value, storeUpd.AddMail.Tag, true)
			if err != nil {
				stdlog.Fatalf("[imap.ApplyStore] STORE fail: %v", err)
			}
		}

		for msgNum, msgName := range node.MailboxContents[storeUpd.User][storeUpd.Mailbox] {

			// Find old mail file's sequence number.
			if msgName == storeUpd.RmvMail[0].Value {

				// Replace old file name with renamed new one.
				node.MailboxContents[storeUpd.User][storeUpd.Mailbox][msgNum] = storeUpd.AddMail.Value
			}
		}
	}
}

// ApplyCRDTUpd receives strings representing CRDT
// update operations from receiver and executes them.
func (node *IMAPNode) ApplyCRDTUpd(applyChan chan string, doneChan chan struct{}) {

	for {

		// Receive update message from receiver
		// via channel.
		updMsg := <-applyChan

		// Parse operation that payload specifies.
		op, opPayload, err := comm.ParseOp(updMsg)
		if err != nil {
			stdlog.Fatalf("[imap.ApplyCRDTUpd] Error while parsing operation from sync message: %v", err)
		}

		// Depending on received operation,
		// parse remaining payload further.
		switch op {

		case "create":
			node.ApplyCreate(opPayload)

		case "delete":
			node.ApplyDelete(opPayload)

		case "append":
			node.ApplyAppend(opPayload)

		case "expunge":
			node.ApplyExpunge(opPayload)

		case "store":
			node.ApplyStore(opPayload)
		}

		// Signal receiver that update was performed.
		doneChan <- struct{}{}
	}
}
