package imap

import (
	"fmt"
	"log"
	"os"
	"strings"

	"path/filepath"

	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/crdt"
)

// Functions

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

	log.Printf("Serving CREATE '%s'...\n", req.Tag)

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

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

	log.Printf("Serving DELETE '%s'...\n", req.Tag)

	// Lock worker exclusively and unlock whenever
	// this handler exits.
	worker.lock.Lock()
	defer worker.lock.Unlock()

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
