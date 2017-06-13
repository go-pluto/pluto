package imap

import (
	"fmt"
	"io"
	stdlog "log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"crypto/tls"
	"io/ioutil"
	"path/filepath"

	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crdt"
)

// Structs

// IMAPNode unifies needed management elements
// for nodes types worker and storage. This allows
// for one single place to define behaviour of
// handling IMAP as well as CRDT update requests.
type IMAPNode struct {
	Lock               *sync.RWMutex
	MailboxStructure   map[string]map[string]*crdt.ORSet
	MailboxContents    map[string]map[string][]string
	CRDTLayerRoot      string
	MaildirRoot        string
	HierarchySeparator string
}

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
func (node *IMAPNode) Select(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	if (c.State != Authenticated) && (c.State != Mailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command SELECT cannot be executed in this state", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	if len(req.Payload) < 1 {

		// If no mailbox to select was specified in payload,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command SELECT was sent without a mailbox to select", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every whitespace character.
	mailboxes := strings.Split(req.Payload, " ")

	if len(mailboxes) != 1 {

		// If there were more than two names supplied to select,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command SELECT was sent with multiple mailbox names instead of only one", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Save maildir for later use.
	mailboxPath := c.UserMaildirPath

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
		err := c.InternalSend(true, fmt.Sprintf("%s NO SELECT failure, not a valid Maildir folder", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Set selected mailbox in connection to supplied one
	// and advance IMAP state of connection to Mailbox.
	c.State = Mailbox
	c.SelectedMailbox = mailboxes[0]

	node.Lock.RLock()
	defer node.Lock.RUnlock()

	// Store contents structure of selected mailbox
	// for later convenient use.
	selMailboxContents := node.MailboxContents[c.UserName][c.SelectedMailbox]

	// Count how many mails do not have the \Seen
	// flag attached, i.e. recent mails.
	recentMails := 0
	for _, mail := range selMailboxContents {

		// Retrieve flags of mail.
		mailFlags, err := mailbox.Flags(mail, false)
		if err != nil {
			c.Error("Error while retrieving flags for mail", err)
			return false
		}

		// Check if \Seen flag is present.
		if strings.ContainsRune(mailFlags, 'S') != true {
			recentMails++
		}
	}

	// TODO: Add other SELECT response elements if needed.

	// Send answer to requesting client.
	err = c.InternalSend(true, fmt.Sprintf("* %d EXISTS\r\n* %d RECENT\r\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\r\n%s OK [READ-WRITE] SELECT completed", len(selMailboxContents), recentMails, req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Create attempts to create a mailbox with name
// taken from payload of request.
func (node *IMAPNode) Create(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	if (c.State != Authenticated) && (c.State != Mailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command APPEND cannot be executed in this state", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every space character.
	posMailboxes := strings.Split(req.Payload, " ")

	if len(posMailboxes) != 1 {

		// If payload did not contain exactly one element,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command CREATE was not sent with exactly one parameter", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Trim supplied mailbox name of hierarchy separator if
	// it was sent with a trailing one.
	posMailbox := strings.TrimSuffix(posMailboxes[0], node.HierarchySeparator)

	if strings.ToUpper(posMailbox) == "INBOX" {

		// If mailbox to-be-created was named INBOX,
		// this is a client error. Return NO response.
		err := c.InternalSend(true, fmt.Sprintf("%s NO New mailbox cannot be named INBOX", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Build up paths before entering critical section.
	posMaildir := maildir.Dir(filepath.Join(c.UserMaildirPath, posMailbox))
	posMailboxCRDTPath := filepath.Join(c.UserCRDTPath, fmt.Sprintf("%s.log", posMailbox))

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	node.Lock.Lock()
	defer node.Lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[c.UserName]["Structure"]

	if userMainCRDT.Lookup(posMailbox) {

		// If mailbox to-be-created already exists for user,
		// this is a client error. Return NO response.
		err := c.InternalSend(true, fmt.Sprintf("%s NO New mailbox cannot be named after already existing mailbox", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Create a new Maildir on stable storage.
	err := posMaildir.Create()
	if err != nil {
		c.Error("Error while creating Maildir for new mailbox", err)
		return false
	}

	// Initialize new ORSet for new mailbox.
	posMailboxCRDT, err := crdt.InitORSetWithFile(posMailboxCRDTPath)
	if err != nil {

		// Perform clean up.
		stdlog.Printf("[imap.Create] Fail: %v", err)
		stdlog.Printf("[imap.Create] Removing just created Maildir completely...")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			stdlog.Printf("[imap.Create] ... failed to remove Maildir: %v", err)
			stdlog.Printf("[imap.Create] Exiting")
		} else {
			stdlog.Printf("[imap.Create] ... done - exiting")
		}

		os.Exit(1)
	}

	// Place newly created CRDT in mailbox structure.
	node.MailboxStructure[c.UserName][posMailbox] = posMailboxCRDT

	// Initialize contents slice for new mailbox to track
	// message sequence numbers in it.
	node.MailboxContents[c.UserName][posMailbox] = make([]string, 0, 6)

	// If succeeded, add a new folder in user's main CRDT
	// and synchronise it to other replicas.
	err = userMainCRDT.Add(posMailbox, func(args ...string) {
		syncChan <- comm.Msg{
			Operation: "create",
			Create: &comm.Msg_CREATE{
				User:    c.UserName,
				Mailbox: posMailbox,
				AddMailbox: &comm.Msg_Element{
					Value: args[0],
					Tag:   args[1],
				},
			},
		}
	})
	if err != nil {

		// Perform clean up.
		stdlog.Printf("[imap.Create] Fail: %v", err)
		stdlog.Printf("[imap.Create] Removing added CRDT from mailbox structure...")

		// Remove just added CRDT of new maildir from mailbox structure
		// and corresponding contents slice.
		delete(node.MailboxStructure[c.UserName], posMailbox)
		delete(node.MailboxContents[c.UserName], posMailbox)

		stdlog.Printf("[imap.Create] ... done. Removing just created Maildir completely...")

		// Attempt to remove Maildir.
		err = posMaildir.Remove()
		if err != nil {
			stdlog.Printf("[imap.Create] ... failed to remove Maildir: %v", err)
			stdlog.Printf("[imap.Create] Exiting")
		} else {
			stdlog.Printf("[imap.Create] ... done - exiting")
		}

		os.Exit(1)
	}

	// Send success answer.
	err = c.InternalSend(true, fmt.Sprintf("%s OK CREATE completed", req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Delete attempts to remove an existing mailbox with
// all included content in CRDT as well as file system.
func (node *IMAPNode) Delete(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	if (c.State != Authenticated) && (c.State != Mailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command DELETE cannot be executed in this state", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every space character.
	delMailboxes := strings.Split(req.Payload, " ")

	if len(delMailboxes) != 1 {

		// If payload did not contain exactly one element,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command DELETE was not sent with exactly one parameter", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Trim supplied mailbox name of hierarchy separator if
	// it was sent with a trailing one.
	delMailbox := strings.TrimSuffix(delMailboxes[0], node.HierarchySeparator)

	if strings.ToUpper(delMailbox) == "INBOX" {

		// If mailbox to-be-deleted was named INBOX,
		// this is a client error. Return NO response.
		err := c.InternalSend(true, fmt.Sprintf("%s NO Forbidden to delete INBOX", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Build up paths before entering critical section.
	delMailboxCRDTPath := filepath.Join(c.UserCRDTPath, fmt.Sprintf("%s.log", delMailbox))
	delMaildir := maildir.Dir(filepath.Join(c.UserMaildirPath, delMailbox))

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	node.Lock.Lock()
	defer node.Lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[c.UserName]["Structure"]

	// TODO: Add routines to take care of mailboxes that
	//       are tagged with a \Noselect tag.

	// Remove element from user's main CRDT and send out
	// remove update operations to all other replicas.
	err := userMainCRDT.Remove(delMailbox, func(args ...string) {

		// Prepare slice of Element structs to capture
		// all value-tag-pairs to remove.
		rmvMailbox := make([]*comm.Msg_Element, (len(args) / 2))

		for i := 0; i < (len(args) / 2); i++ {

			rmvMailbox[i] = &comm.Msg_Element{
				Value: args[(2 * i)],
				Tag:   args[((2 * i) + 1)],
			}
		}

		syncChan <- comm.Msg{
			Operation: "delete",
			Delete: &comm.Msg_DELETE{
				User:       c.UserName,
				Mailbox:    delMailbox,
				RmvMailbox: rmvMailbox,
			},
		}
	})
	if err != nil {

		// Check if error was caused by client, trying to
		// delete an non-existent mailbox.
		if err.Error() == "element to be removed not found in set" {

			// If so, return a NO response.
			err := c.InternalSend(true, fmt.Sprintf("%s NO Cannot delete folder that does not exist", req.Tag), false, "")
			if err != nil {
				c.Error("Encountered send error", err)
				return false
			}

			return true
		}

		// Otherwise, this is a write-back error of the updated CRDT
		// log file. Reverting actions were already taken, log error.
		stdlog.Printf("[imap.Delete] Failed to remove elements from user's main CRDT: %v", err)

		os.Exit(1)
	}

	// Remove CRDT from mailbox structure and corresponding
	// mail contents slice.
	delete(node.MailboxStructure[c.UserName], delMailbox)
	delete(node.MailboxContents[c.UserName], delMailbox)

	// Remove CRDT file of mailbox.
	err = os.Remove(delMailboxCRDTPath)
	if err != nil {
		// TODO: Maybe think about better way to clean up here?
		c.Error("Error while deleting CRDT file of mailbox", err)
		return false
	}

	// Remove files associated with deleted mailbox
	// from stable storage.
	err = delMaildir.Remove()
	if err != nil {
		// TODO: Maybe think about better way to clean up here?
		c.Error("Error while deleting Maildir", err)
		return false
	}

	// Send success answer.
	err = c.InternalSend(true, fmt.Sprintf("%s OK DELETE completed", req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// List allows clients to learn about the mailboxes
// available and also returns the hierarchy delimiter.
func (node *IMAPNode) List(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	if (c.State != Authenticated) && (c.State != Mailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command LIST cannot be executed in this state", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every space character.
	listArgs := strings.Split(req.Payload, " ")

	if len(listArgs) != 2 {

		// If payload did not contain between exactly two elements,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command LIST was not sent with exactly two arguments", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	if (listArgs[1] != "%") && (listArgs[1] != "*") {

		// If second argument is not one of two wildcards,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command LIST needs either '%%' or '*' as mailbox name", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	node.Lock.RLock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[c.UserName]

	// Reserve space for answer.
	listAnswerLines := make([]string, 0, (len(userMainCRDT) - 1))

	for mailbox := range userMainCRDT {

		// Do not consider structure element.
		if mailbox != "Structure" {

			// Split currently considered mailbox name at
			// defined hierarchy separator.
			mailboxParts := strings.Split(mailbox, node.HierarchySeparator)

			if (listArgs[1] == "*") || (len(mailboxParts) == 1) {

				// Either always include a mailbox in the response
				// or only when it is a top level mailbox.
				listAnswerLines = append(listAnswerLines, fmt.Sprintf("* LIST () \"%s\" %s", node.HierarchySeparator, mailbox))
			}
		}
	}

	node.Lock.RUnlock()

	// Send out LIST response lines.
	for _, listAnswerLine := range listAnswerLines {

		err := c.InternalSend(true, listAnswerLine, false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}
	}

	// Send success answer.
	err := c.InternalSend(true, fmt.Sprintf("%s OK LIST completed", req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Append puts supplied message into specified mailbox.
func (node *IMAPNode) Append(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	var mailbox string
	var flagsRaw string
	var dateTimeRaw string
	var numBytesRaw string

	if (c.State != Authenticated) && (c.State != Mailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command APPEND cannot be executed in this state", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Split payload on every space character.
	appendArgs := strings.Split(req.Payload, " ")
	lenAppendArgs := len(appendArgs)

	if (lenAppendArgs < 2) || (lenAppendArgs > 4) {

		// If payload did not contain between two and four
		// elements, this is a client error.
		// Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command APPEND was not sent with appropriate number of parameters", req.Tag), false, "")
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

		_, err := ParseFlags(flagsRaw)
		if err != nil {

			// Parsing flags from APPEND request produced
			// an error. Return tagged BAD response.
			err := c.InternalSend(true, fmt.Sprintf("%s BAD %s", req.Tag, err.Error()), false, "")
			if err != nil {
				c.Error("Encountered send error", err)
				return false
			}

			return true
		}

		// TODO: Do something with these flags.
	}

	// If date-time was supplied, parse it.
	if dateTimeRaw != "" {
		// TODO: Parse time and do something with it.
	}

	// Parse out how many bytes we are expecting.
	numBytesString := strings.TrimPrefix(numBytesRaw, "{")
	numBytesString = strings.TrimSuffix(numBytesString, "}")

	// Convert string number to int.
	numBytes, err := strconv.Atoi(numBytesString)
	if err != nil {

		// If we were not able to parse out the number,
		// it was probably a client error. Send tagged BAD.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command APPEND did not contain proper literal data byte number", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Construct path to maildir on node.
	var appMaildir maildir.Dir
	if mailbox == "INBOX" {
		appMaildir = maildir.Dir(c.UserMaildirPath)
	} else {
		appMaildir = maildir.Dir(filepath.Join(c.UserMaildirPath, mailbox))
	}

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	node.Lock.Lock()
	defer node.Lock.Unlock()

	// Save user's mailbox structure CRDT to more
	// conveniently use it hereafter.
	userMainCRDT := node.MailboxStructure[c.UserName]["Structure"]

	if userMainCRDT.Lookup(mailbox) != true {

		// If mailbox to append message to does not exist,
		// this is a client error. Return NO response.
		err := c.InternalSend(true, fmt.Sprintf("%s NO [TRYCREATE] Mailbox to append to does not exist", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Send command continuation to client.
	err = c.InternalSend(true, "+ Ready for literal data", false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Signal proxying distributor that we expect an
	// inbound answer from the client.
	err = c.SignalAwaitingLiteral(true, numBytes)
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Reserve space for exact amount of expected data.
	msgBuffer := make([]byte, numBytes)

	// Read in that amount from connection to distributor.
	_, err = io.ReadFull(c.IncReader, msgBuffer)
	if err != nil {
		c.Error("Encountered error while reading distributor literal data", err)
		return false
	}

	// Open a new Maildir delivery.
	appDelivery, err := appMaildir.NewDelivery()
	if err != nil {
		c.Error("Error during delivery creation", err)
		return false
	}

	// Write actual message content to file.
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
	appMailboxCRDT := node.MailboxStructure[c.UserName][mailbox]

	// Append new mail to mailbox' contents CRDT.
	node.MailboxContents[c.UserName][mailbox] = append(node.MailboxContents[c.UserName][mailbox], mailFileName)

	// Add new mail to mailbox' CRDT and send update
	// message to other replicas.
	err = appMailboxCRDT.Add(mailFileName, func(args ...string) {
		syncChan <- comm.Msg{
			Operation: "append",
			Append: &comm.Msg_APPEND{
				User:    c.UserName,
				Mailbox: mailbox,
				AddMail: &comm.Msg_Element{
					Value:    args[0],
					Tag:      args[1],
					Contents: msgBuffer,
				},
			},
		}
	})
	if err != nil {

		// Perform clean up.
		stdlog.Printf("[imap.Append] Fail: %v", err)
		stdlog.Printf("[imap.Append] Removing just appended mail message...")

		err := os.Remove(mailFileNamePath)
		if err != nil {
			stdlog.Printf("[imap.Append] ... failed: %v", err)
			stdlog.Printf("[imap.Append] Exiting")
		} else {
			stdlog.Printf("[imap.Append] ... done - exiting")
		}

		os.Exit(1)
	}

	// Send success answer.
	err = c.InternalSend(true, fmt.Sprintf("%s OK APPEND completed", req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Expunge deletes messages permanently from currently
// selected mailbox that have been flagged as Deleted
// prior to calling this function.
func (node *IMAPNode) Expunge(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	if c.State != Mailbox {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD No mailbox selected to expunge", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	if len(req.Payload) > 0 {

		// If payload was not empty to EXPUNGE command,
		// this is a client error. Return BAD statement.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command EXPUNGE was sent with extra parameters", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Construct path to Maildir to expunge.
	var expMaildir maildir.Dir
	if c.SelectedMailbox == "INBOX" {
		expMaildir = maildir.Dir(filepath.Join(c.UserMaildirPath, "cur"))
	} else {
		expMaildir = maildir.Dir(filepath.Join(c.UserMaildirPath, c.SelectedMailbox, "cur"))
	}

	// Reserve space for mails to expunge.
	expMailNums := make([]int, 0, 6)

	// Declare variable to contain answers of
	// individual remove operations.
	var expAnswerLines []string

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	node.Lock.Lock()

	// Save all mails possibly to delete and
	// number of these files.
	expMails := node.MailboxContents[c.UserName][c.SelectedMailbox]
	numExpMails := len(expMails)

	// Only do the work if there are any mails
	// present in mailbox.
	if numExpMails > 0 {

		// Iterate over all mail files in reverse order.
		for i := (numExpMails - 1); i >= 0; i-- {

			// Retrieve all flags of fetched mail.
			mailFlags, err := expMaildir.Flags(expMails[i], false)
			if err != nil {
				c.Error("Encountered error while retrieving flags for expunging mails", err)
				node.Lock.Unlock()
				return false
			}

			// Check for presence of \Deleted flag and
			// add to delete set if found.
			if strings.ContainsRune(mailFlags, 'T') {
				expMailNums = append(expMailNums, i)
			}
		}

		// Reserve space for answer lines.
		expAnswerLines = make([]string, 0, len(expMailNums))

		// Retrieve CRDT of mailbox to expunge.
		expMailboxCRDT := node.MailboxStructure[c.UserName][c.SelectedMailbox]

		for _, msgNum := range expMailNums {

			// Remove each mail to expunge from mailbox CRDT.
			err := expMailboxCRDT.Remove(expMails[msgNum], func(args ...string) {

				// Prepare slice of Element structs to capture
				// all value-tag-pairs to remove.
				rmvMails := make([]*comm.Msg_Element, (len(args) / 2))

				for i := 0; i < (len(args) / 2); i++ {

					rmvMails[i] = &comm.Msg_Element{
						Value: args[(2 * i)],
						Tag:   args[((2 * i) + 1)],
					}
				}

				syncChan <- comm.Msg{
					Operation: "expunge",
					Expunge: &comm.Msg_EXPUNGE{
						User:    c.UserName,
						Mailbox: c.SelectedMailbox,
						RmvMail: rmvMails,
					},
				}
			})
			if err != nil {

				// This is a write-back error of the updated mailbox CRDT
				// log file. Reverting actions were already taken, log error.
				stdlog.Printf("[imap.Expunge] Failed to remove mails from user's selected mailbox CRDT: %v", err)
				node.Lock.Unlock()
				os.Exit(1)
			}

			// Construct path to file.
			expMailPath := filepath.Join(string(expMaildir), expMails[msgNum])

			// Remove the file.
			err = os.Remove(expMailPath)
			if err != nil {
				c.Error("Error while removing expunged mail file from stable storage", err)
				node.Lock.Unlock()
				return false
			}

			// Immediately remove mail from contents structure.
			realMsgNum := msgNum + 1
			expMails = append(expMails[:msgNum], expMails[realMsgNum:]...)

			// Append individual remove answer to answer lines.
			expAnswerLines = append(expAnswerLines, fmt.Sprintf("* %d EXPUNGE", realMsgNum))
		}

		node.Lock.Unlock()

		// Send out FETCH part with new flags.
		for _, expAnswerLine := range expAnswerLines {

			err := c.InternalSend(true, expAnswerLine, false, "")
			if err != nil {
				c.Error("Encountered send error", err)
				return false
			}
		}
	}

	// Send success answer.
	err := c.InternalSend(true, fmt.Sprintf("%s OK EXPUNGE completed", req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Store takes in message sequence numbers and some set
// of flags to change in those messages and changes the
// attributes for these mails throughout the system.
func (node *IMAPNode) Store(c *IMAPConnection, req *Request, syncChan chan comm.Msg) bool {

	// Set updated flags list indicator
	// initially to false.
	silent := false

	if c.State != Mailbox {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD No mailbox selected for store", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
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
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Command STORE was not sent with three parameters", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Parse data item type (second parameter).
	dataItemType := storeArgs[1]

	if (dataItemType != "FLAGS") && (dataItemType != "FLAGS.SILENT") &&
		(dataItemType != "+FLAGS") && (dataItemType != "+FLAGS.SILENT") &&
		(dataItemType != "-FLAGS") && (dataItemType != "-FLAGS.SILENT") {

		// If supplied data item type is none of the
		// supported ones, this is a client error.
		// Send tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD Unknown data item type specified", req.Tag), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// If client requested not to receive the updated
	// flags list, set indicator to false.
	if strings.HasSuffix(dataItemType, ".SILENT") {
		silent = true
	}

	// Parse flag argument (third parameter).
	flags, err := ParseFlags(storeArgs[2])
	if err != nil {

		// Parsing flags from STORE request produced
		// an error. Return tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD %s", req.Tag, err.Error()), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Set currently selected mailbox with respect to special
	// case of INBOX as current location.
	var selectedMailbox string
	if c.SelectedMailbox == "INBOX" {
		selectedMailbox = c.UserMaildirPath
	} else {
		selectedMailbox = filepath.Join(c.UserMaildirPath, c.SelectedMailbox)
	}

	// Build up paths before entering critical section.
	mailMaildir := maildir.Dir(selectedMailbox)

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	node.Lock.Lock()

	// Retrieve number of messages in mailbox.
	lenMailboxContents := len(node.MailboxContents[c.UserName][c.SelectedMailbox])

	// Parse sequence numbers argument (first parameter).
	// CAUTION: We expect this function to fail if supplied
	//          message sequence numbers did not refer to
	//          existing messages in mailbox.
	msgNums, err := ParseSeqNumbers(storeArgs[0], lenMailboxContents)
	if err != nil {

		// Parsing sequence numbers from STORE request produced
		// an error. Return tagged BAD response.
		err := c.InternalSend(true, fmt.Sprintf("%s BAD %s", req.Tag, err.Error()), false, "")
		if err != nil {
			c.Error("Encountered send error", err)
			node.Lock.Unlock()
			return false
		}

		node.Lock.Unlock()

		return true
	}

	// Prepare answer slice.
	answerLines := make([]string, 0, len(msgNums))

	for _, msgNum := range msgNums {

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
		mailFileName := node.MailboxContents[c.UserName][c.SelectedMailbox][msgNum]

		// Read message content from file.
		mailFileContent, err := ioutil.ReadFile(filepath.Join(selectedMailbox, "cur", mailFileName))
		if err != nil {
			c.Error("Error while reading in mail file content in STORE operation", err)
			node.Lock.Unlock()
			return false
		}

		// Retrieve flags included in mail file name.
		mailFlags, err := mailMaildir.Flags(mailFileName, false)
		if err != nil {
			c.Error("Error while retrieving flags from mail file", err)
			node.Lock.Unlock()
			return false
		}

		// Check if supplied flags should be added
		// to existing flags if not yet present.
		if (dataItemType == "+FLAGS") || (dataItemType == "+FLAGS.SILENT") {

			newMailFlagsString := string(newMailFlags)

			for _, char := range mailFlags {

				// Iterate over all characters of the currently present
				// flags of the mail and check if they are present in
				// new flags string as well. If not, add it.
				if strings.ContainsRune(newMailFlagsString, char) != true {
					newMailFlags = append(newMailFlags, char)
				}
			}
		}

		// Check if supplied flags should be removed
		// from existing flags if they are present.
		if (dataItemType == "-FLAGS") || (dataItemType == "-FLAGS.SILENT") {

			tmpNewMailFlags := mailFlags

			for _, char := range newMailFlags {

				// Iterate over all characters of the supplied new
				// flags and check if they are present in current
				// flags string. If so, remove them from intermediate
				// variable that holds former flags.
				if strings.ContainsRune(mailFlags, char) {
					tmpNewMailFlags = strings.Replace(tmpNewMailFlags, string(char), "", -1)
				}
			}

			// Reset new flags slice to reduced intermediate value.
			newMailFlags = []rune(tmpNewMailFlags)
		}

		// Check if we really have to perform an update
		// across the system or if we can save the energy.
		if mailFlags != string(newMailFlags) {

			// Set just constructed new flags string in mail's
			// file name (renaming it).
			newMailFileName, err := mailMaildir.SetFlags(mailFileName, string(newMailFlags), false)
			if err != nil {
				c.Error("Error renaming mail file in STORE operation", err)
				node.Lock.Unlock()
				return false
			}

			// Save CRDT of mailbox.
			storeMailboxCRDT := node.MailboxStructure[c.UserName][c.SelectedMailbox]

			var rmvMails []*comm.Msg_Element

			// First, remove the former name of the mail file
			// but do not yet send out an update operation.
			err = storeMailboxCRDT.Remove(mailFileName, func(args ...string) {

				// Prepare slice of Element structs to capture
				// all value-tag-pairs to remove.
				rmvMails = make([]*comm.Msg_Element, (len(args) / 2))

				for i := 0; i < (len(args) / 2); i++ {

					rmvMails[i] = &comm.Msg_Element{
						Value: args[(2 * i)],
						Tag:   args[((2 * i) + 1)],
					}
				}
			})
			if err != nil {

				// This is a write-back error of the updated mailbox CRDT
				// log file. Reverting actions were already taken, log error.
				stdlog.Printf("[imap.Store] Failed to remove old mail name from selected mailbox CRDT: %v", err)
				node.Lock.Unlock()
				os.Exit(1)
			}

			// Second, add the new mail file's name and finally
			// instruct all other nodes to do the same.
			err = storeMailboxCRDT.Add(newMailFileName, func(args ...string) {

				syncChan <- comm.Msg{
					Operation: "store",
					Store: &comm.Msg_STORE{
						User:    c.UserName,
						Mailbox: c.SelectedMailbox,
						RmvMail: rmvMails,
						AddMail: &comm.Msg_Element{
							Value:    args[0],
							Tag:      args[1],
							Contents: mailFileContent,
						},
					},
				}
			})
			if err != nil {

				// This is a write-back error of the updated mailbox CRDT
				// log file. Reverting actions were already taken, log error.
				stdlog.Printf("[imap.Store] Failed to add renamed mail name to selected mailbox CRDT: %v", err)
				node.Lock.Unlock()
				os.Exit(1)
			}

			// If we are done with that, also replace the mail's
			// file name in the corresponding contents slice.
			node.MailboxContents[c.UserName][c.SelectedMailbox][msgNum] = newMailFileName
		}

		// Check if client requested update information.
		if silent != true {

			// Build new flags list for answer.
			answerFlagString := ""

			for _, newFlag := range newMailFlags {

				switch newFlag {

				case 'D':

					if answerFlagString == "" {
						answerFlagString = "\\Draft"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Draft", answerFlagString)
					}

				case 'F':

					if answerFlagString == "" {
						answerFlagString = "\\Flagged"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Flagged", answerFlagString)
					}

				case 'R':

					if answerFlagString == "" {
						answerFlagString = "\\Answered"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Answered", answerFlagString)
					}

				case 'S':

					if answerFlagString == "" {
						answerFlagString = "\\Seen"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Seen", answerFlagString)
					}

				case 'T':

					if answerFlagString == "" {
						answerFlagString = "\\Deleted"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Deleted", answerFlagString)
					}

				}

			}

			// Append this file's FETCH answer.
			realMsgNum := msgNum + 1
			answerLines = append(answerLines, fmt.Sprintf("* %d FETCH (FLAGS (%s))", realMsgNum, answerFlagString))
		}
	}

	node.Lock.Unlock()

	// Check if client requested update information.
	if silent != true {

		// Send out FETCH part with new flags.
		for _, answerLine := range answerLines {

			err = c.InternalSend(true, answerLine, false, "")
			if err != nil {
				c.Error("Encountered send error", err)
				return false
			}
		}
	}

	// Send success answer.
	err = c.InternalSend(true, fmt.Sprintf("%s OK STORE completed", req.Tag), false, "")
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}
