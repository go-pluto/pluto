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

	// Write new mailbox' file to stable storage.
	err = posMailboxCRDT.WriteORSetToFile()
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
	userMainCRDT.Add(posMailbox, func(payload string) {
		worker.SyncSendChan <- fmt.Sprintf("create|%s||%s|", worker.Contexts[clientID].UserName, payload)
	})

	// Write updated CRDT to stable storage.
	err = userMainCRDT.WriteORSetToFile()
	if err != nil {

		// Perform clean up.

		log.Printf("[imap.Create] Fail: %s\n", err.Error())
		log.Printf("[imap.Create] Deleting just added mailbox from main structure CRDT...\n")

		// Immediately send out remove operation.
		userMainCRDT.Remove(posMailbox, func(payload string) {
			worker.SyncSendChan <- payload
		})

		log.Printf("[imap.Create] ... done.\n")
		log.Printf("[imap.Create] Removing just created Maildir completely...\n")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			log.Printf("[imap.Create] ... failed to remove Maildir: %s\n", err.Error())
		} else {
			log.Printf("[imap.Create] ... done.\n")
		}

		log.Printf("[imap.Create] Removing just created CRDT file...\n")

		// Attempt to remove just created CRDT file.
		err = os.Remove(posMailboxCRDTPath)
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
