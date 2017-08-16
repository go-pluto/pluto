package imap

import (
	"os"

	"path/filepath"

	"github.com/go-kit/kit/log/level"
	"github.com/go-pluto/maildir"
	"github.com/go-pluto/pluto/comm"
)

// ApplyCreate performs the downstream part
// of a CREATE operation.
func (mailbox *Mailbox) ApplyCreate(createUpd *comm.Msg_CREATE) {

	createMaildir := filepath.Join(mailbox.MaildirPath, createUpd.Mailbox)

	// We need to track existence state of various
	// file system objects in case we need to revert.
	maildirExisted := true
	msgSeqNumExisted := true

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Only attempt to create the corresponding
	// Maildir if it does not already exist.
	_, err := os.Stat(createMaildir)
	if os.IsNotExist(err) {

		maildirExisted = false

		// Create a new Maildir on stable storage.
		err = maildir.Dir(createMaildir).Create()
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "maildir for new mailbox folder could not be created",
				"err", err,
			)
			os.Exit(1)
		}
	}

	// If no slice was found in mail message structure,
	// initialize one for new mailbox to track message
	// sequence numbers in it.
	_, found := mailbox.Mails[createUpd.Mailbox]
	if !found {
		msgSeqNumExisted = false
		mailbox.Mails[createUpd.Mailbox] = make([]string, 0, 6)
	}

	// If succeeded, add a new folder in user's main CRDT.
	err = mailbox.Structure.AddEffect(createUpd.AddMailbox.Value, createUpd.AddMailbox.Tag, true)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "fail during downstream CREATE execution, will clean up",
			"err", err,
		)

		// If it did not exist, remove the just
		// added slice from mail message map.
		if !msgSeqNumExisted {
			delete(mailbox.Mails, createUpd.Mailbox)
		}

		// If it did not exist, attempt to remove
		// the created Maildir.
		if !maildirExisted {

			err = maildir.Dir(createMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}
}

// ApplyDelete performs the downstream part
// of a DELETE operation.
func (mailbox *Mailbox) ApplyDelete(deleteUpd *comm.Msg_DELETE) {

	delMaildir := filepath.Join(mailbox.MaildirPath, deleteUpd.Mailbox)

	rmElements := make(map[string]string)
	for _, element := range deleteUpd.RmvMailbox {
		rmElements[element.Tag] = element.Value
	}

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Remove received pairs from user's main CRDT.
	err := mailbox.Structure.RemoveEffect(rmElements, true)
	if err != nil {
		level.Error(mailbox.Logger).Log(
			"msg", "failed to remove elements of mailbox folder to delete from user's structure CRDT",
			"err", err,
		)
		os.Exit(1)
	}

	if mailbox.Structure.Lookup(deleteUpd.Mailbox) {

		// Concurrent IMAP operations have declared interest in
		// this mailbox by adding elements to the structure CRDT.
		// Do not remove the underlying files. Instead, delete
		// the mail files sent by the source node as representing
		// the folder's content at the time of DELETE issuance.

		for _, mail := range deleteUpd.RmvMails {

			var delFileName string
			if deleteUpd.Mailbox == "INBOX" {
				delFileName = filepath.Join(mailbox.MaildirPath, "cur", mail)
			} else {
				delFileName = filepath.Join(mailbox.MaildirPath, deleteUpd.Mailbox, "cur", mail)
			}

			// In that case, delete the file system object.
			err := os.Remove(delFileName)
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove an underlying mail file during downstream DELETE execution",
					"err", err,
				)
				os.Exit(1)
			}

			// As well as the mail's entries in the
			// internal sequence number representation.
			for msgNum, msgName := range mailbox.Mails[deleteUpd.Mailbox] {

				if msgName == mail {

					realMsgNum := msgNum + 1
					mailbox.Mails[deleteUpd.Mailbox] = append(mailbox.Mails[deleteUpd.Mailbox][:msgNum], mailbox.Mails[deleteUpd.Mailbox][realMsgNum:]...)
				}
			}
		}

	} else {

		// This DELETE operation removed the entire presence of
		// this folder from the user's mailbox. Thus, file system
		// clean up of files and folders, and internal state
		// representation manipulation is due.

		// Remove slice from contents map if present.
		_, found := mailbox.Mails[deleteUpd.Mailbox]
		if found {
			delete(mailbox.Mails, deleteUpd.Mailbox)
		}

		// Remove files associated with deleted mailbox
		// from stable storage, if present.
		_, err = os.Stat(delMaildir)
		if err == nil {

			err = maildir.Dir(delMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove Maildir during downstream DELETE execution",
					"err", err,
				)
				os.Exit(1)
			}
		}
	}
}

// ApplyAppend performs the downstream part
// of an APPEND operation.
func (mailbox *Mailbox) ApplyAppend(appendUpd *comm.Msg_APPEND) {

	// For APPEND, STORE, and EXPUNGE we interpret the
	// the folder name as value and the mail file name
	// as tag in downstream message.

	appendMaildir := mailbox.MaildirPath
	var appendFileName string

	if appendUpd.Mailbox == "INBOX" {
		appendFileName = filepath.Join(mailbox.MaildirPath, "cur", appendUpd.AddMail.Tag)
	} else {
		appendMaildir = filepath.Join(mailbox.MaildirPath, appendUpd.Mailbox)
		appendFileName = filepath.Join(mailbox.MaildirPath, appendUpd.Mailbox, "cur", appendUpd.AddMail.Tag)
	}

	// We need to track if we had to create the
	// mailbox folder in case we need to revert.
	createdMailbox := false

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Check if the specified mailbox folder to append the message to
	// is not present. If that is the case, create the mailbox folder.
	if !mailbox.Structure.Lookup(appendUpd.Mailbox) {

		createdMailbox = true

		_, err := os.Stat(appendMaildir)
		if os.IsNotExist(err) {

			err = maildir.Dir(appendMaildir).Create()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "missing mailbox folder could not be created in downstream APPEND operation",
					"err", err,
				)
				os.Exit(1)
			}
		}

		_, found := mailbox.Mails[appendUpd.Mailbox]
		if !found {
			mailbox.Mails[appendUpd.Mailbox] = make([]string, 0, 6)
		}
	}

	// Create a file system object under correct name.
	appendFile, err := os.Create(appendFileName)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to create file for mail to append in downstream APPEND operation",
			"err", err,
		)

		// If we had to create the mailbox folder,
		// remove that state again.
		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND operation",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Write received message content to created file.
	_, err = appendFile.Write(appendUpd.AddMail.Contents)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to write message content in downstream APPEND operation",
			"err", err,
		)

		// Remove just created mail file.
		err = os.Remove(appendFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream APPEND operation",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND operation",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Sync content to stable storage.
	err = appendFile.Sync()
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to sync message to stable storage in downstream APPEND operation",
			"err", err,
		)

		err = os.Remove(appendFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream APPEND operation",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND operation",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Append new mail file name to message sequence
	// numbers tracking structure.
	// Mind: tag in this case means mail file name.
	mailbox.Mails[appendUpd.Mailbox] = append(mailbox.Mails[appendUpd.Mailbox], appendUpd.AddMail.Tag)

	// Declare interest of the APPEND operation in the involved
	// mailbox folder by putting the (folder, mail file name)
	// pair into the structure OR-Set.
	err = mailbox.Structure.AddEffect(appendUpd.AddMail.Value, appendUpd.AddMail.Tag, true)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to manipulate structure OR-Set in downstream APPEND operation",
			"err", err,
		)

		err = os.Remove(appendFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream APPEND operation",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND operation",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}
}

// ApplyExpunge performs the downstream part
// of an EXPUNGE operation.
func (mailbox *Mailbox) ApplyExpunge(expungeUpd *comm.Msg_EXPUNGE) {

	rmElements := make(map[string]string)
	for _, element := range expungeUpd.RmvMail {
		rmElements[element.Tag] = element.Value
	}

	var delFileName string
	if expungeUpd.Mailbox == "INBOX" {
		delFileName = filepath.Join(mailbox.MaildirPath, "cur", expungeUpd.RmvMail[0].Value)
	} else {
		delFileName = filepath.Join(mailbox.MaildirPath, expungeUpd.Mailbox, "cur", expungeUpd.RmvMail[0].Value)
	}

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Check if specified mailbox from expunge message is
	// present in user's main CRDT on this node.
	if mailbox.MailboxStructure[expungeUpd.User]["Structure"].Lookup(expungeUpd.Mailbox) {

		// Delete supplied elements from mailbox.
		err := mailbox.MailboxStructure[expungeUpd.User][expungeUpd.Mailbox].RemoveEffect(rmElements, true)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove mail elements from respective mailbox CRDT",
				"err", err,
			)
			os.Exit(1)
		}

		// Check if just removed elements marked all
		// instances of mail file.
		if mailbox.MailboxStructure[expungeUpd.User][expungeUpd.Mailbox].Lookup(expungeUpd.RmvMail[0].Value) != true {

			// If that is the case, remove the file.
			err := os.Remove(delFileName)
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove underlying mail file during downstream EXPUNGE execution",
					"err", err,
				)
				os.Exit(1)
			}
		}

		for msgNum, msgName := range mailbox.MailboxContents[expungeUpd.User][expungeUpd.Mailbox] {

			// Find removed mail file's sequence number.
			if msgName == expungeUpd.RmvMail[0].Value {

				// Delete mail's sequence number from contents structure.
				realMsgNum := msgNum + 1
				mailbox.MailboxContents[expungeUpd.User][expungeUpd.Mailbox] = append(mailbox.MailboxContents[expungeUpd.User][expungeUpd.Mailbox][:msgNum], mailbox.MailboxContents[expungeUpd.User][expungeUpd.Mailbox][realMsgNum:]...)
			}
		}
	}
}

// ApplyStore performs the downstream part
// of a STORE operation.
func (mailbox *Mailbox) ApplyStore(storeUpd *comm.Msg_STORE) {

	rmElements := make(map[string]string)
	for _, element := range storeUpd.RmvMail {
		rmElements[element.Tag] = element.Value
	}

	var delFileName string
	if storeUpd.Mailbox == "INBOX" {
		delFileName = filepath.Join(mailbox.MaildirPath, "cur", storeUpd.RmvMail[0].Value)
	} else {
		delFileName = filepath.Join(mailbox.MaildirPath, storeUpd.Mailbox, "cur", storeUpd.RmvMail[0].Value)
	}

	storeMaildir := mailbox.MaildirPath
	var storeFileName string

	if storeUpd.Mailbox == "INBOX" {
		storeFileName = filepath.Join(mailbox.MaildirPath, "cur", storeUpd.AddMail.Value)
	} else {
		storeMaildir = filepath.Join(mailbox.MaildirPath, storeUpd.Mailbox)
		storeFileName = filepath.Join(mailbox.MaildirPath, storeUpd.Mailbox, "cur", storeUpd.AddMail.Value)
	}

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Check if specified mailbox from store message is present
	// in user's main CRDT on this node.
	if mailbox.MailboxStructure[storeUpd.User]["Structure"].Lookup(storeUpd.Mailbox) {

		// Delete supplied elements from mailbox.
		err := mailbox.MailboxStructure[storeUpd.User][storeUpd.Mailbox].RemoveEffect(rmElements, true)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove mail elements from respective mailbox CRDT",
				"err", err,
			)
			os.Exit(1)
		}

		// Check if just removed elements marked all
		// instances of mail file.
		if mailbox.MailboxStructure[storeUpd.User][storeUpd.Mailbox].Lookup(storeUpd.RmvMail[0].Value) != true {

			// If that is the case, remove the file.
			err := os.Remove(delFileName)
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove underlying mail file during downstream STORE execution",
					"err", err,
				)
				os.Exit(1)
			}
		}

		// Check if new mail name is not yet present
		// on this node.
		if mailbox.MailboxStructure[storeUpd.User][storeUpd.Mailbox].Lookup(storeUpd.AddMail.Value) != true {

			// If not yet present on node, place file
			// content at correct location.
			storeFile, err := os.Create(storeFileName)
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to create mail file during downstream STORE execution",
					"err", err,
				)
				os.Exit(1)
			}

			_, err = storeFile.Write(storeUpd.AddMail.Contents)
			if err != nil {

				level.Error(mailbox.Logger).Log(
					"msg", "fail during downstream STORE execution, will clean up",
					"err", err,
				)

				// Remove just created mail file.
				err = os.Remove(storeFileName)
				if err != nil {
					level.Error(mailbox.Logger).Log(
						"msg", "failed to remove created mail file",
						"err", err,
					)
				}

				os.Exit(1)
			}

			// Sync content to stable storage.
			err = storeFile.Sync()
			if err != nil {

				level.Error(mailbox.Logger).Log(
					"msg", "fail during downstream STORE execution, will clean up",
					"err", err,
				)

				// Remove just created mail file.
				err = os.Remove(storeFileName)
				if err != nil {
					level.Error(mailbox.Logger).Log(
						"msg", "failed to remove created mail file",
						"err", err,
					)
				}

				os.Exit(1)
			}

			// If succeeded, add renamed mail to mailbox' CRDT.
			err = mailbox.MailboxStructure[storeUpd.User][storeUpd.Mailbox].AddEffect(storeUpd.AddMail.Value, storeUpd.AddMail.Tag, true)
			if err != nil {

				level.Error(mailbox.Logger).Log(
					"msg", "fail during downstream STORE execution, will clean up",
					"err", err,
				)

				// Remove just created mail file.
				err = os.Remove(storeFileName)
				if err != nil {
					level.Error(mailbox.Logger).Log(
						"msg", "failed to remove created mail file",
						"err", err,
					)
				}

				os.Exit(1)
			}
		} else {

			// Add renamed mail to mailbox' CRDT.
			err = mailbox.MailboxStructure[storeUpd.User][storeUpd.Mailbox].AddEffect(storeUpd.AddMail.Value, storeUpd.AddMail.Tag, true)
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "fail during downstream STORE execution",
					"err", err,
				)
				os.Exit(1)
			}
		}

		for msgNum, msgName := range mailbox.MailboxContents[storeUpd.User][storeUpd.Mailbox] {

			// Find old mail file's sequence number.
			if msgName == storeUpd.RmvMail[0].Value {

				// Replace old file name with renamed new one.
				mailbox.MailboxContents[storeUpd.User][storeUpd.Mailbox][msgNum] = storeUpd.AddMail.Value
			}
		}
	}
}
