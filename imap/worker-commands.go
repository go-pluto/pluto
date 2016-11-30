package imap

import (
	"fmt"
	"log"
	"strings"

	"path/filepath"

	"github.com/numbleroot/maildir"
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

	worker.lock.Lock()
	defer worker.lock.Unlock()

	// Check if connection is in correct state.
	if (worker.Contexts[clientID].IMAPState != AUTHENTICATED) && (worker.Contexts[clientID].IMAPState != MAILBOX) {
		return false
	}

	// Save maildir for later use.
	mailbox := worker.Contexts[clientID].UserMaildir

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
func (worker *Worker) Create(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Append inserts a message built from further supplied
// message literal in a mailbox specified in payload.
func (worker *Worker) Append(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Store updates meta data of specified messages and
// returns the new meta data of those messages.
func (worker *Worker) Store(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Copy takes specified messages and inserts them again
// into another stated mailbox.
func (worker *Worker) Copy(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Expunge deletes messages prior marked as to-be-deleted
// via labelling them with the \Deleted flag.
func (worker *Worker) Expunge(c *Connection, req *Request, ctx *Context) bool {

	return true
}
