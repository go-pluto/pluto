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

	log.Println()
	log.Printf("Serving SELECT '%s'...\n", req.Tag)

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

	// Check if connection is in correct state.
	if (worker.Contexts[clientID].IMAPState != AUTHENTICATED) && (worker.Contexts[clientID].IMAPState != MAILBOX) {
		return false
	}

	// Save maildir for later use.
	mailbox := worker.Contexts[clientID].UserMaildirPath

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
		mailbox = maildir.Dir(filepath.Join(string(mailbox), mailboxes[0]))
	}

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
	worker.Contexts[clientID].SelectedMailbox = mailbox

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

	log.Println()
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

	// TODO: Check and handle UIDVALIDITY behaviour is correct.
	//       I. a. make sure to assign correct new UIDs.

	// TODO: RFC 3501 says that not yet existing parent folders
	//       of a mailbox name containing at least one hierarchy
	//       symbol 'SHOULD' be created as well. It is certainly
	//       not required in pluto but there might other servers
	//       or agents which might require such additional beahviour.
	//       Think about adding this functionality and remember to
	//       adjust RENAME behaviour as well.

	// Create a new Maildir on stable storage.
	posMaildir := maildir.Dir(filepath.Join(string(worker.Contexts[clientID].UserMaildirPath), posMailbox))

	err := posMaildir.Create()
	if err != nil {
		log.Fatalf("[imap.Create] Maildir for new mailbox could not be created: %s\n", err.Error())
	}

	// Construct path to new CRDT file.
	posMailboxCRDTPath := filepath.Join(worker.Contexts[clientID].UserCRDTPath, fmt.Sprintf("%s.log", posMailbox))

	// Initialize new ORSet for new mailbox.
	_, err = crdt.InitORSetWithFile(posMailboxCRDTPath)
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

	// If succeeded, add a new folder in user's main CRDT
	// and synchronise it to other replicas.
	err = userMainCRDT.Add(posMailbox, func(payload string) {
		worker.SyncSendChan <- fmt.Sprintf("create|%s|%s|%s", worker.Contexts[clientID].UserName, posMailbox, payload)
	})
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

	log.Println()
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
		worker.SyncSendChan <- fmt.Sprintf("delete|%s|%s|%s", worker.Contexts[clientID].UserName, delMailbox, payload)
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
	delMaildir := maildir.Dir(filepath.Join(string(worker.Contexts[clientID].UserMaildirPath), delMailbox))

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

// Rename attempts to rename a supplied mailbox to
// a new name including all possibly contained subfolders.
func (worker *Worker) Rename(c *Connection, req *Request, clientID string) bool {

	/*
		log.Println()
		log.Printf("Serving RENAME '%s'...\n", req.Tag)

		// Split payload on every space character.
		renMailboxes := strings.Split(req.Payload, " ")

		if len(renMailboxes) != 2 {

			// If payload did not contain exactly two elements,
			// this is a client error. Return BAD statement.
			err := c.Send(fmt.Sprintf("%s BAD Command RENAME was not sent with exactly two parameters", req.Tag))
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

		// Trim supplied mailbox names of hierarchy separator if
		// it they were sent with a trailing one.
		oldMailbox := strings.TrimSuffix(renMailboxes[0], worker.Config.IMAP.HierarchySeparator)
		newMailbox := strings.TrimSuffix(renMailboxes[1], worker.Config.IMAP.HierarchySeparator)

		// TODO: Add missing behaviour.

	*/

	// Send success answer.
	err := c.Send(fmt.Sprintf("%s OK RENAME completed", req.Tag))
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
	var flags []string
	var dateTimeRaw string
	var numBytesRaw string

	log.Println()
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

	mailbox = strings.ToUpper(mailbox)

	// If flags were supplied, parse them.
	if flagsRaw != "" {

		// Remove leading and trailing parenthesis.
		flagsRaw = strings.TrimLeft(flagsRaw, "(")
		flagsRaw = strings.TrimRight(flagsRaw, ")")

		// Split at space symbols.
		flags = strings.Split(flagsRaw, " ")

		// TODO: Handle errors.
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
		appMaildir = worker.Contexts[clientID].UserMaildirPath
	} else {
		appMaildir = maildir.Dir(filepath.Join(string(worker.Contexts[clientID].UserMaildirPath), mailbox))
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

	// Add new mail to mailbox' CRDT and send update
	// message to other replicas.
	err = appMailboxCRDT.Add(mailFileName, func(payload string) {
		worker.SyncSendChan <- fmt.Sprintf("append|%s|%s|%s;%s", worker.Contexts[clientID].UserName, mailbox, payload, base64.StdEncoding.EncodeToString(msgBuffer))
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
