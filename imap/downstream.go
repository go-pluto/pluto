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
				"msg", "maildir for new mailbox folder could not be created in downstream CREATE execution",
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

	// Add a new mailbox folder in structure CRDT.
	err = mailbox.Structure.AddEffect(createUpd.Mailbox, createUpd.AddTag, true)
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
					"msg", "failed to remove created Maildir during clean up of failed downstream CREATE execution",
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
	for _, tag := range deleteUpd.RmvTags {
		rmElements[tag] = deleteUpd.Mailbox
	}

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Remove received pairs from structure CRDT.
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

			// Delete the file system object.
			err := os.Remove(delFileName)
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove an underlying mail file in downstream DELETE execution",
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

		// This DELETE operation removed the entire presence of this
		// mailbox folder from the user's mailbox. Thus, file system
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
					"msg", "failed to remove Maildir in downstream DELETE execution",
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

	// We need to track if we had to create the
	// mailbox folder in case we need to revert.
	createdMailbox := false

	appendMaildir := mailbox.MaildirPath
	var appendFileName string

	if appendUpd.Mailbox == "INBOX" {
		appendFileName = filepath.Join(mailbox.MaildirPath, "cur", appendUpd.AddTag)
	} else {
		appendMaildir = filepath.Join(mailbox.MaildirPath, appendUpd.Mailbox)
		appendFileName = filepath.Join(mailbox.MaildirPath, appendUpd.Mailbox, "cur", appendUpd.AddTag)
	}

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
					"msg", "missing mailbox folder could not be created in downstream APPEND execution",
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
			"msg", "failed to create file for mail to append in downstream APPEND execution",
			"err", err,
		)

		// If we had to create the mailbox folder,
		// remove that state again.
		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Write received message content to created file.
	_, err = appendFile.Write(appendUpd.AddContent)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to write message content in downstream APPEND execution",
			"err", err,
		)

		// Remove just created mail file.
		err = os.Remove(appendFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream APPEND execution",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND execution",
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
			"msg", "failed to sync message to stable storage in downstream APPEND execution",
			"err", err,
		)

		err = os.Remove(appendFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream APPEND execution",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Append new mail file name to message sequence
	// numbers tracking structure.
	// Mind: tag in this case means mail file name.
	mailbox.Mails[appendUpd.Mailbox] = append(mailbox.Mails[appendUpd.Mailbox], appendUpd.AddTag)

	// Declare interest of the APPEND operation in the involved
	// mailbox folder by putting the mailbox-file-name pair
	// into the structure OR-Set.
	err = mailbox.Structure.AddEffect(appendUpd.Mailbox, appendUpd.AddTag, true)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to update structure OR-Set in downstream APPEND execution",
			"err", err,
		)

		err = os.Remove(appendFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream APPEND execution",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, appendUpd.Mailbox)

			err = maildir.Dir(appendMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream APPEND execution",
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

	createdMailbox := false

	rmElements := map[string]string{
		expungeUpd.RmvTag: expungeUpd.Mailbox,
	}

	expungeMaildir := mailbox.MaildirPath
	var delFileName string

	if expungeUpd.Mailbox == "INBOX" {
		delFileName = filepath.Join(mailbox.MaildirPath, "cur", expungeUpd.RmvTag)
	} else {
		expungeMaildir = filepath.Join(mailbox.MaildirPath, expungeUpd.Mailbox)
		delFileName = filepath.Join(mailbox.MaildirPath, expungeUpd.Mailbox, "cur", expungeUpd.RmvTag)
	}

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	err := mailbox.Structure.RemoveEffect(rmElements, true)
	if err != nil {
		level.Error(mailbox.Logger).Log(
			"msg", "failed to remove mail elements from structure CRDT in downstream EXPUNGE execution",
			"err", err,
		)
		os.Exit(1)
	}

	for msgNum, msgName := range mailbox.Mails[expungeUpd.Mailbox] {

		// Find removed mail file's sequence number.
		if msgName == expungeUpd.RmvTag {

			// Delete mail's sequence number from message
			// sequence number tracking structure.
			realMsgNum := msgNum + 1
			mailbox.Mails[expungeUpd.Mailbox] = append(mailbox.Mails[expungeUpd.Mailbox][:msgNum], mailbox.Mails[expungeUpd.Mailbox][realMsgNum:]...)
		}
	}

	// Remove the respective mail message file.
	err = os.Remove(delFileName)
	if err != nil {

		// Only an error not related to the non-existence
		// of the file is an error we need to handle.
		if !os.IsNotExist(err) {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove underlying mail file in downstream EXPUNGE execution",
				"err", err,
			)
			os.Exit(1)
		}
	}

	// Check if the specified mailbox folder to remove the message from
	// is not present. If that is the case, create the mailbox folder.
	if !mailbox.Structure.Lookup(expungeUpd.Mailbox) {

		createdMailbox = true

		_, err := os.Stat(expungeMaildir)
		if os.IsNotExist(err) {

			err = maildir.Dir(expungeMaildir).Create()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "missing mailbox folder could not be created in downstream EXPUNGE execution",
					"err", err,
				)
				os.Exit(1)
			}
		}

		_, found := mailbox.Mails[expungeUpd.Mailbox]
		if !found {
			mailbox.Mails[expungeUpd.Mailbox] = make([]string, 0, 6)
		}
	}

	// Add the mailbox-addTag pair to structure CRDT.
	// This declares the interest of this operation in
	// the upper-level mailbox folder.
	err = mailbox.Structure.AddEffect(expungeUpd.Mailbox, expungeUpd.AddTag, true)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "fail during downstream EXPUNGE execution, will clean up",
			"err", err,
		)

		// If we had to create the mailbox folder,
		// remove that state again.
		if createdMailbox {

			delete(mailbox.Mails, expungeUpd.Mailbox)

			err = maildir.Dir(expungeMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream EXPUNGE execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}
}

// ApplyStore performs the downstream part
// of a STORE operation.
func (mailbox *Mailbox) ApplyStore(storeUpd *comm.Msg_STORE) {

	createdMailbox := false

	rmElements := map[string]string{
		storeUpd.RmvTag: storeUpd.Mailbox,
	}

	storeMaildir := mailbox.MaildirPath
	var delFileName string
	var storeFileName string

	if storeUpd.Mailbox == "INBOX" {
		delFileName = filepath.Join(mailbox.MaildirPath, "cur", storeUpd.RmvTag)
		storeFileName = filepath.Join(mailbox.MaildirPath, "cur", storeUpd.AddTag)
	} else {
		storeMaildir = filepath.Join(mailbox.MaildirPath, storeUpd.Mailbox)
		delFileName = filepath.Join(mailbox.MaildirPath, storeUpd.Mailbox, "cur", storeUpd.RmvTag)
		storeFileName = filepath.Join(mailbox.MaildirPath, storeUpd.Mailbox, "cur", storeUpd.AddTag)
	}

	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	err := mailbox.Structure.RemoveEffect(rmElements, true)
	if err != nil {
		level.Error(mailbox.Logger).Log(
			"msg", "failed to remove mail elements from structure CRDT in downstream STORE execution",
			"err", err,
		)
		os.Exit(1)
	}

	// Remove the respective mail message file.
	err = os.Remove(delFileName)
	if err != nil {

		// Only an error not related to the non-existence
		// of the file is an error we need to handle.
		if !os.IsNotExist(err) {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove underlying mail file in downstream STORE execution",
				"err", err,
			)
			os.Exit(1)
		}
	}

	// Check if the specified mailbox folder to store the message to is
	// not present. If that is the case, create the mailbox folder.
	if !mailbox.Structure.Lookup(storeUpd.Mailbox) {

		createdMailbox = true

		_, err := os.Stat(storeMaildir)
		if os.IsNotExist(err) {

			err = maildir.Dir(storeMaildir).Create()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "missing mailbox folder could not be created in downstream STORE execution",
					"err", err,
				)
				os.Exit(1)
			}
		}

		_, found := mailbox.Mails[storeUpd.Mailbox]
		if !found {
			mailbox.Mails[storeUpd.Mailbox] = make([]string, 0, 6)
		}
	}

	// Create a file system object under correct name.
	storeFile, err := os.Create(storeFileName)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to create file for mail to append in downstream STORE execution",
			"err", err,
		)

		// If we had to create the mailbox folder,
		// remove that state again.
		if createdMailbox {

			delete(mailbox.Mails, storeUpd.Mailbox)

			err = maildir.Dir(storeMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream STORE execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Write received message content to created file.
	_, err = storeFile.Write(storeUpd.AddContent)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to write message content in downstream STORE execution",
			"err", err,
		)

		// Remove just created mail file.
		err = os.Remove(storeFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream STORE execution",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, storeUpd.Mailbox)

			err = maildir.Dir(storeMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream STORE execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Sync content to stable storage.
	err = storeFile.Sync()
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to sync message to stable storage in downstream STORE execution",
			"err", err,
		)

		err = os.Remove(storeFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream STORE execution",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, storeUpd.Mailbox)

			err = maildir.Dir(storeMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream STORE execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	// Add the mailbox-new-mail-name pair into
	// the structure CRDT.
	err = mailbox.Structure.AddEffect(storeUpd.Mailbox, storeUpd.AddTag, true)
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "failed to update structure OR-Set in downstream STORE execution",
			"err", err,
		)

		err = os.Remove(storeFileName)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail file during clean up of failed downstream STORE execution",
				"err", err,
			)
		}

		if createdMailbox {

			delete(mailbox.Mails, storeUpd.Mailbox)

			err = maildir.Dir(storeMaildir).Remove()
			if err != nil {
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove created Maildir during clean up of failed downstream STORE execution",
					"err", err,
				)
			}
		}

		os.Exit(1)
	}

	for msgNum, msgName := range mailbox.Mails[storeUpd.Mailbox] {

		// Find old mail file's sequence number.
		if msgName == storeUpd.RmvTag {

			// Replace old file name with new one.
			mailbox.Mails[storeUpd.Mailbox][msgNum] = storeUpd.AddTag
		}
	}
}
