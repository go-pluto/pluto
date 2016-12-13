package imap

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"encoding/base64"
	"path/filepath"

	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/crdt"
)

// Functions

// Mailbox functions

// Select sets the current mailbox based on supplied
// payload to user-instructed value. A return value of
// this function does not indicate whether the command
// was successfully handled according to IMAP semantics,
// but rather whether a fatal error occurred or a complete
// answer could been sent. So, in case of an user error
// (e.g. a missing mailbox to select) but otherwise correct
// handling, this function would send a useful message to
// the client and still return true.
func (worker *Worker) Select(c *Connection, req *Request, clientID string) bool {

	log.Printf("Serving SELECT '%s'...\n", req.Tag)

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	if (worker.Contexts[clientID].IMAPState != AUTHENTICATED) && (worker.Contexts[clientID].IMAPState != MAILBOX) {

		// If connection was not correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.Send(fmt.Sprintf("%s BAD Command SELECT cannot be executed in this state", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return true
	}

	// Save maildir for later use.
	mailboxPath := worker.Contexts[clientID].UserMaildirPath

	if len(req.Payload) < 1 {

		// If no mailbox to select was specified in payload,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command SELECT was sent without a mailbox to select", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every whitespace character.
	mailboxes := strings.Split(req.Payload, " ")

	if len(mailboxes) != 1 {

		// If there were more than two names supplied to select,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command SELECT was sent with multiple mailbox names instead of only one", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return true
	}

	// If any other mailbox than INBOX was specified,
	// append it to mailbox in order to check it.
	if mailboxes[0] != "INBOX" {
		mailboxPath = filepath.Join(mailboxPath, mailboxes[0])
	}

	// Transform into real Maildir.
	mailbox := maildir.Dir(mailboxPath)

	// Check if mailbox is existing and a conformant maildir folder.
	err := mailbox.Check()
	if err != nil {

		// If specified maildir did not turn out to be a valid one,
		// this is a client error. Return NO statement.
		err := c.Send(fmt.Sprintf("%s NO SELECT failure, not a valid Maildir folder", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return true
	}

	// Set selected mailbox in context to supplied one
	// and advance IMAP state of connection to MAILBOX.
	worker.Contexts[clientID].IMAPState = MAILBOX
	worker.Contexts[clientID].SelectedMailbox = filepath.Base(mailboxPath)

	// Build up answer to client.
	answer := ""

	// Include part for standard flags.
	answer = answer + "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\n"
	answer = answer + "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]"

	// TODO: Add all other required answer parts.

	// Send prepared answer to requesting client.
	err = c.Send(answer)
	if err != nil {
		c.ErrorLogOnly("Encountered send error", err)
		return false
	}

	return true
}

// Create attempts to create a mailbox with name
// taken from payload of request.
func (worker *Worker) Create(c *Connection, req *Request, clientID string) bool {

	log.Printf("Serving CREATE '%s'...\n", req.Tag)

	// Split payload on every space character.
	posMailboxes := strings.Split(req.Payload, " ")

	if len(posMailboxes) != 1 {

		// If payload did not contain exactly one element,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command CREATE was not sent with exactly one parameter", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	// Trim supplied mailbox name of hierarchy separator if
	// it was sent with a trailing one.
	posMailbox := strings.TrimSuffix(posMailboxes[0], worker.Config.IMAP.HierarchySeparator)

	if strings.ToUpper(posMailbox) == "INBOX" {

		// If mailbox to-be-created was named INBOX,
		// this is a client error. Return NO response.
		err := c.Send(fmt.Sprintf("%s NO New mailbox cannot be named INBOX", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := worker.MailboxStructure[worker.Contexts[clientID].UserName]["Structure"]

	if userMainCRDT.Lookup(posMailbox, true) {

		// If mailbox to-be-created already exists for user,
		// this is a client error. Return NO response.
		err := c.Send(fmt.Sprintf("%s NO New mailbox cannot be named after already existing mailbox", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Create a new Maildir on stable storage.
	posMaildir := maildir.Dir(filepath.Join(worker.Contexts[clientID].UserMaildirPath, posMailbox))

	err := posMaildir.Create()
	if err != nil {
		log.Fatalf("[imap.Create] Maildir for new mailbox could not be created: %s\n", err.Error())
	}

	// Construct path to new CRDT file.
	posMailboxCRDTPath := filepath.Join(worker.Contexts[clientID].UserCRDTPath, fmt.Sprintf("%s.log", posMailbox))

	// Initialize new ORSet for new mailbox.
	posMailboxCRDT, err := crdt.InitORSetWithFile(posMailboxCRDTPath)
	if err != nil {

		// Perform clean up.
		log.Printf("[imap.Create] Fail: %s\n", err.Error())
		log.Printf("[imap.Create] Removing just created Maildir completely...\n")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			log.Printf("[imap.Create] ... failed to remove Maildir: %s\n", err.Error())
			log.Printf("[imap.Create] Exiting.\n")
		} else {
			log.Printf("[imap.Create] ... done. Exiting.\n")
		}

		// Exit worker.
		os.Exit(1)
	}

	// Place newly created CRDT in mailbox structure.
	worker.MailboxStructure[worker.Contexts[clientID].UserName][posMailbox] = posMailboxCRDT

	// Initialize contents slice for new mailbox to track
	// message sequence numbers in it.
	worker.MailboxContents[worker.Contexts[clientID].UserName][posMailbox] = make([]string, 0, 6)

	// If succeeded, add a new folder in user's main CRDT
	// and synchronise it to other replicas.
	err = userMainCRDT.Add(posMailbox, func(payload string) {
		worker.SyncSendChan <- fmt.Sprintf("create|%s|%s|%s", worker.Contexts[clientID].UserName, base64.StdEncoding.EncodeToString([]byte(posMailbox)), payload)
	})
	if err != nil {

		// Perform clean up.
		log.Printf("[imap.Create] Fail: %s\n", err.Error())
		log.Printf("[imap.Create] Removing added CRDT from mailbox structure...\n")

		// Remove just added CRDT of new maildir from mailbox structure
		// and corresponding contents slice.
		delete(worker.MailboxStructure[worker.Contexts[clientID].UserName], posMailbox)
		delete(worker.MailboxContents[worker.Contexts[clientID].UserName], posMailbox)

		log.Printf("[imap.Create] ... done. Removing just created Maildir completely...\n")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			log.Printf("[imap.Create] ... failed to remove Maildir: %s\n", err.Error())
			log.Printf("[imap.Create] Exiting.\n")
		} else {
			log.Printf("[imap.Create] ... done. Exiting.\n")
		}

		// Exit worker.
		os.Exit(1)
	}

	// Send success answer.
	err = c.Send(fmt.Sprintf("%s OK CREATE completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Delete attempts to remove an existing mailbox with
// all included content in CRDT as well as file system.
func (worker *Worker) Delete(c *Connection, req *Request, clientID string) bool {

	log.Printf("Serving DELETE '%s'...\n", req.Tag)

	// Split payload on every space character.
	delMailboxes := strings.Split(req.Payload, " ")

	if len(delMailboxes) != 1 {

		// If payload did not contain exactly one element,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command DELETE was not sent with exactly one parameter", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	// Trim supplied mailbox name of hierarchy separator if
	// it was sent with a trailing one.
	delMailbox := strings.TrimSuffix(delMailboxes[0], worker.Config.IMAP.HierarchySeparator)

	if strings.ToUpper(delMailbox) == "INBOX" {

		// If mailbox to-be-deleted was named INBOX,
		// this is a client error. Return NO response.
		err := c.Send(fmt.Sprintf("%s NO Forbidden to delete INBOX", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := worker.MailboxStructure[worker.Contexts[clientID].UserName]["Structure"]

	// TODO: Add routines to take care of mailboxes that
	//       are tagged with a \Noselect tag.

	// Remove element from user's main CRDT and send out
	// remove update operations to all other replicas.
	err := userMainCRDT.Remove(delMailbox, func(payload string) {
		worker.SyncSendChan <- fmt.Sprintf("delete|%s|%s|%s", worker.Contexts[clientID].UserName, base64.StdEncoding.EncodeToString([]byte(delMailbox)), payload)
	})
	if err != nil {

		// Check if error was caused by client, trying to
		// delete an non-existent mailbox.
		if err.Error() == "element to be removed not found in set" {

			// If so, return a NO response.
			err := c.Send(fmt.Sprintf("%s NO Cannot delete folder that does not exist", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return false
			}

			return true
		}

		// Otherwise, this is a write-back error of the updated CRDT
		// log file. Reverting actions were already taken, log error.
		log.Printf("[imap.Delete] Failed to remove elements from user's main CRDT: %s\n", err.Error())

		// Exit worker.
		os.Exit(1)
	}

	// Remove CRDT from mailbox structure and corresponding
	// mail contents slice.
	delete(worker.MailboxStructure[worker.Contexts[clientID].UserName], delMailbox)
	delete(worker.MailboxContents[worker.Contexts[clientID].UserName], delMailbox)

	// Construct path to CRDT file to delete.
	delMailboxCRDTPath := filepath.Join(worker.Contexts[clientID].UserCRDTPath, fmt.Sprintf("%s.log", delMailbox))

	// Remove CRDT file of mailbox.
	err = os.Remove(delMailboxCRDTPath)
	if err != nil {
		// TODO: Maybe think about better way to clean up here?
		log.Fatalf("[imap.Delete] CRDT file of mailbox could not be deleted: %s\n", err.Error())
	}

	// Remove files associated with deleted mailbox
	// from stable storage.
	delMaildir := maildir.Dir(filepath.Join(worker.Contexts[clientID].UserMaildirPath, delMailbox))

	err = delMaildir.Remove()
	if err != nil {
		// TODO: Maybe think about better way to clean up here?
		log.Fatalf("[imap.Delete] Maildir could not be deleted: %s\n", err.Error())
	}

	// Send success answer.
	err = c.Send(fmt.Sprintf("%s OK DELETE completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Mail functions

// Append puts supplied message into specified mailbox.
func (worker *Worker) Append(c *Connection, req *Request, clientID string) bool {

	// Arguments of append command.
	var mailbox string
	var flagsRaw string
	var dateTimeRaw string
	var numBytesRaw string

	log.Printf("Serving APPEND '%s'...\n", req.Tag)

	// Split payload on every space character.
	appendArgs := strings.Split(req.Payload, " ")
	lenAppendArgs := len(appendArgs)

	if (lenAppendArgs < 2) || (lenAppendArgs > 4) {

		// If payload did not contain between two and four
		// elements, this is a client error.
		// Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command APPEND was not sent with appropriate number of parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Depending on amount of arguments, store them.
	switch lenAppendArgs {

	case 2:
		mailbox = appendArgs[0]
		numBytesRaw = appendArgs[1]

	case 3:
		mailbox = appendArgs[0]
		flagsRaw = appendArgs[1]
		numBytesRaw = appendArgs[2]

	case 4:
		mailbox = appendArgs[0]
		flagsRaw = appendArgs[1]
		dateTimeRaw = appendArgs[2]
		numBytesRaw = appendArgs[3]
	}

	// If user specified inbox, set it accordingly.
	if strings.ToUpper(mailbox) == "INBOX" {
		mailbox = "INBOX"
	}

	// If flags were supplied, parse them.
	if flagsRaw != "" {

		flags, err := ParseFlags(flagsRaw)
		if err != nil {

			// Parsing flags from APPEND request produced
			// an error. Return tagged BAD response.
			err := c.Send(fmt.Sprintf("%s BAD %s", req.Tag, err.Error()))
			if err != nil {
				c.Error("Encountered send error", err)
				return false
			}

			return true
		}

		log.Printf("flags: %#v\n", flags)
	}

	// If date-time was supplied, parse it.
	if dateTimeRaw != "" {

		// TODO: Parse time and do something with it.
	}

	// Parse out how many bytes we are expecting.
	numBytesString := strings.TrimLeft(numBytesRaw, "{")
	numBytesString = strings.TrimRight(numBytesString, "}")

	// Convert string number to int.
	numBytes, err := strconv.Atoi(numBytesString)
	if err != nil {

		// If we were not able to parse out the number,
		// it was probably a client error. Send tagged BAD.
		err := c.Send(fmt.Sprintf("%s BAD Command APPEND did not contain proper literal data byte number", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Lock worker.
	worker.lock.Lock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := worker.MailboxStructure[worker.Contexts[clientID].UserName]["Structure"]

	if userMainCRDT.Lookup(mailbox, true) != true {

		// If mailbox to append message to does not exist,
		// this is a client error. Return NO response.
		err := c.Send(fmt.Sprintf("%s NO [TRYCREATE] Mailbox to append to does not exist", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			worker.lock.Unlock()
			return false
		}

		// Unlock worker again.
		worker.lock.Unlock()

		return true
	}

	// Unlock worker again.
	worker.lock.Unlock()

	// Send command continuation to client.
	err = c.Send("+ Ready for literal data")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Signal proxying distributor that we expect an
	// inbound answer from the client.
	err = c.SignalAwaitingLiteral(numBytes)
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Reserve space for exact amount of expected data.
	msgBuffer := make([]byte, numBytes)

	// Read in that amount from connection to distributor.
	_, err = io.ReadFull(c.Reader, msgBuffer)
	if err != nil {
		c.Error("Encountered error while reading distributor literal data", err)
		return false
	}

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	// Construct path to maildir on storage.
	var appMaildir maildir.Dir
	if mailbox == "INBOX" {
		appMaildir = maildir.Dir(worker.Contexts[clientID].UserMaildirPath)
	} else {
		appMaildir = maildir.Dir(filepath.Join(worker.Contexts[clientID].UserMaildirPath, mailbox))
	}

	// Open a new Maildir delivery.
	appDelivery, err := appMaildir.NewDelivery()
	if err != nil {
		c.Error("Error during delivery creation", err)
		return false
	}

	// Write actual message contents to file.
	err = appDelivery.Write(msgBuffer)
	if err != nil {
		c.Error("Error while writing message during delivery", err)
		return false
	}

	// Close and move just created message.
	newKey, err := appDelivery.Close()
	if err != nil {
		c.Error("Error while finishing delivery of new message", err)
		return false
	}

	// Follow Maildir's renaming procedure.
	_, err = appMaildir.Unseen()
	if err != nil {
		c.Error("Could not Unseen() recently delivered messages", err)
		return false
	}

	// Find file name of just delivered mail.
	mailFileNamePath, err := appMaildir.Filename(newKey)
	if err != nil {
		c.Error("Finding file name of new message failed", err)
		return false
	}
	mailFileName := filepath.Base(mailFileNamePath)

	// Retrieve CRDT of mailbox to append mail to.
	appMailboxCRDT := worker.MailboxStructure[worker.Contexts[clientID].UserName][mailbox]

	// Append new mail to mailbox' contents CRDT.
	worker.MailboxContents[worker.Contexts[clientID].UserName][mailbox] = append(worker.MailboxContents[worker.Contexts[clientID].UserName][mailbox], mailFileName)

	// Add new mail to mailbox' CRDT and send update
	// message to other replicas.
	err = appMailboxCRDT.Add(mailFileName, func(payload string) {
		worker.SyncSendChan <- fmt.Sprintf("append|%s|%s|%s;%s", worker.Contexts[clientID].UserName, base64.StdEncoding.EncodeToString([]byte(mailbox)), payload, base64.StdEncoding.EncodeToString(msgBuffer))
	})
	if err != nil {

		// Perform clean up.
		log.Printf("[imap.Append] Fail: %s\n", err.Error())
		log.Printf("[imap.Append] Removing just appended mail message...\n")

		err := os.Remove(mailFileNamePath)
		if err != nil {
			log.Printf("[imap.Append] ... failed: %s\n", err.Error())
			log.Printf("[imap.Append] Exiting.\n")
		} else {
			log.Printf("[imap.Append] ... done. Exiting.\n")
		}

		// Exit worker.
		os.Exit(1)
	}

	// Send success answer.
	err = c.Send(fmt.Sprintf("%s OK APPEND completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Expunge deletes messages permanently from currently
// selected mailbox that have been flagged as Deleted
// prior to calling this function.
func (worker *Worker) Expunge(c *Connection, req *Request, clientID string) bool {

	log.Printf("Serving EXPUNGE '%s'...\n", req.Tag)

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	if worker.Contexts[clientID].IMAPState != MAILBOX {

		// If connection was not correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.Send(fmt.Sprintf("%s BAD No mailbox selected to expunge", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return true
	}

	// Send success answer.
	err := c.Send(fmt.Sprintf("%s OK EXPUNGE completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Store takes in message sequence numbers and some set
// of flags to change in those messages and changes the
// attributes for these mails throughout the system.
func (worker *Worker) Store(c *Connection, req *Request, clientID string) bool {

	var err error

	// Set updated flags list indicator
	// initially to false.
	silent := false

	log.Printf("Serving STORE '%s'...\n", req.Tag)

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	if worker.Contexts[clientID].IMAPState != MAILBOX {

		// If connection was not correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.Send(fmt.Sprintf("%s BAD No mailbox selected for store", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every space character.
	storeArgs := strings.SplitN(req.Payload, " ", 3)

	if len(storeArgs) < 3 {

		// If payload did not contain at least three
		// elements, this is a client error.
		// Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command STORE was not sent with three parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Parse sequence numbers argument.
	msgNums, err := ParseSeqNumbers(storeArgs[0], worker.MailboxContents[worker.Contexts[clientID].UserName][worker.Contexts[clientID].SelectedMailbox])
	if err != nil {

		// Parsing sequence numbers from STORE request produced
		// an error. Return tagged BAD response.
		err := c.Send(fmt.Sprintf("%s BAD %s", req.Tag, err.Error()))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	log.Printf("msgNums: %#v\n", msgNums)

	// Parse data item type.
	dataItemType := storeArgs[1]

	// If client requested not to receive the updated
	// flags list, set indicator to false.
	if strings.HasSuffix(dataItemType, ".SILENT") {
		silent = true
	}

	log.Printf("dataItemType: %#v\n", dataItemType)
	log.Printf("silent? %#v\n", silent)

	// Parse flag argument.
	flags, err := ParseFlags(storeArgs[2])
	if err != nil {

		// Parsing flags from STORE request produced
		// an error. Return tagged BAD response.
		err := c.Send(fmt.Sprintf("%s BAD %s", req.Tag, err.Error()))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	log.Printf("flags: %#v\n", flags)

	// Construct path and Maildir for selected mailbox.
	mailMaildir := maildir.Dir(filepath.Join(worker.Contexts[clientID].UserMaildirPath, worker.Contexts[clientID].SelectedMailbox))

	for _, msgNum := range msgNums {

		var rmvElements string

		// Initialize runes slice for new flags of mail.
		newMailFlags := make([]rune, 0, 5)

		// Depending on the presence of various standard
		// flags extend newMailFlags further.

		if _, found := flags["\\Draft"]; found {
			newMailFlags = append(newMailFlags, 'D')
		}

		if _, found := flags["\\Flagged"]; found {
			newMailFlags = append(newMailFlags, 'F')
		}

		if _, found := flags["\\Answered"]; found {
			newMailFlags = append(newMailFlags, 'R')
		}

		if _, found := flags["\\Seen"]; found {
			newMailFlags = append(newMailFlags, 'S')
		}

		if _, found := flags["\\Deleted"]; found {
			newMailFlags = append(newMailFlags, 'T')
		}

		// Retrieve mail file name.
		mailFileName := worker.MailboxContents[worker.Contexts[clientID].UserName][worker.Contexts[clientID].SelectedMailbox][msgNum]

		// Retrieve flags included in mail file name.
		mailFlags, err := mailMaildir.Flags(mailFileName, false)
		if err != nil {
			c.Error("Error while retrieving flags from mail file", err)
			return false
		}

		log.Printf("mailFileName: %#v, flags: %#v, newMailFlags: %#v\n", mailFileName, mailFlags, newMailFlags)

		if (dataItemType == "+FLAGS") || (dataItemType == "+FLAGS.SILENT") {

			newMailFlagsString := string(newMailFlags)

			log.Printf("newMailFlagsString: %s\n", newMailFlagsString)

			for _, char := range mailFlags {
				log.Printf("char +: %#v\n", char)

				if strings.ContainsRune(newMailFlagsString, char) != true {
					newMailFlags = append(newMailFlags, char)
				}
			}

			log.Printf("newMailFlags at end of add: %#v\n", newMailFlags)
		}

		if (dataItemType == "-FLAGS") || (dataItemType == "-FLAGS.SILENT") {

			tmpNewMailFlags := mailFlags

			for _, char := range newMailFlags {
				log.Printf("char -: %#v\n", char)

				if strings.ContainsRune(mailFlags, char) {
					tmpNewMailFlags = strings.Replace(tmpNewMailFlags, string(char), "", -1)
				}
			}

			newMailFlags = []rune(tmpNewMailFlags)

			log.Printf("newMailFlags at end of minus: %#v\n", newMailFlags)
		}

		newMailFileName, err := mailMaildir.SetFlags(mailFileName, string(newMailFlags), false)
		if err != nil {
			c.Error("Error renaming mail file in STORE operation", err)
			return false
		}

		log.Printf("newMailFileName: %#v\n", newMailFileName)

		storeMailboxCRDT := worker.MailboxStructure[worker.Contexts[clientID].UserName][worker.Contexts[clientID].SelectedMailbox]
		storeMailboxCRDT.Remove(mailFileName, func(payload string) {
			rmvElements = payload
		})

		storeMailboxCRDT.Add(newMailFileName, func(payload string) {
			worker.SyncSendChan <- fmt.Sprintf("store|%s|%s|%s|%s", worker.Contexts[clientID].UserName, base64.StdEncoding.EncodeToString([]byte(worker.Contexts[clientID].SelectedMailbox)), rmvElements, payload)
		})

		worker.MailboxContents[worker.Contexts[clientID].UserName][worker.Contexts[clientID].SelectedMailbox][msgNum] = newMailFileName
	}

	// Send success answer.
	err = c.Send(fmt.Sprintf("%s OK STORE completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}
