package imap

import (
	"fmt"
	"log"
	"strings"

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
func (node *Node) Select(c *Connection, req *Request, ctx *Context) bool {

	log.Printf("Serving SELECT '%s'...\n", req.Tag)

	// Check if connection is in correct state.
	if (c.IMAPState == ANY) || (c.IMAPState == NOT_AUTHENTICATED) || (c.IMAPState == LOGOUT) {
		return false
	}

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

	// Save supplied maildir.
	mailbox := ctx.UserMaildir

	// If any other mailbox than INBOX was specified,
	// append it to mailbox in order to check it.
	if mailboxes[0] != "INBOX" {
		mailbox = maildir.Dir(fmt.Sprintf("%s%s", ctx.UserMaildir, mailboxes[0]))
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

	// TODO: Deselect any prior selected mailbox in this connection.

	// Set selected mailbox in connection struct to supplied
	// one and advance IMAP state of connection to MAILBOX.
	c.CurrentMailbox = mailbox
	c.IMAPState = MAILBOX

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
func (node *Node) Create(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Append inserts a message built from further supplied
// message literal in a mailbox specified in payload.
func (node *Node) Append(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Store updates meta data of specified messages and
// returns the new meta data of those messages.
func (node *Node) Store(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Copy takes specified messages and inserts them again
// into another stated mailbox.
func (node *Node) Copy(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// Expunge deletes messages prior marked as to-be-deleted
// via labelling them with the \Deleted flag.
func (node *Node) Expunge(c *Connection, req *Request, ctx *Context) bool {

	return true
}

// AcceptWorker is the main worker routine where all
// incoming requests against worker nodes have to go through.
func (node *Node) AcceptWorker(c *Connection) {

	// Connections in this state are only possible against
	// a node of type WORKER, none else.
	if node.Type != WORKER {
		log.Println("[imap.AcceptWorker] DISTRIBUTOR or STORAGE node tried to run this function. Not allowed.")
		return
	}

	// Receive next incoming client command.
	opening, err := c.Receive()
	if err != nil {
		c.ErrorLogOnly("Encountered receive error", err)
		return
	}

	// As long as the distributor node did not indicate
	// that the system is about to shut down, we accept requests.
	for opening != "> done <" {

		// Extract important parts and inject them into struct.
		context, err := ExtractContext(opening)
		if err != nil {
			c.ErrorLogOnly("Error extracting context", err)
			return
		}

		// Receive incoming actual client command.
		rawReq, err := c.Receive()
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			// In case of failure, wait for next sent command.
			rawReq, err = c.Receive()
			if err != nil {
				c.ErrorLogOnly("Encountered receive error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		// Load user-specific environment.
		context.UserMaildir = maildir.Dir(fmt.Sprintf("%s%s/", node.Config.Workers[node.Name].MaildirRoot, context.UserName))
		context.UserCRDT = fmt.Sprintf("%s%s/", node.Config.Workers[node.Name].CRDTLayerRoot, context.UserName)

		switch {

		case rawReq == "> done <":
			// TODO: Trigger state-dependent behaviour when user logged out.
			log.Printf("%s: done.", context.UserName)

		case rawReq == "> changed <":
			// TODO: Trigger state-dependent behaviour when session changed.
			log.Printf("%s: session changed.", context.UserName)

		case req.Command == "SELECT":
			if ok := node.Select(c, req, context); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalSessionDone(nil)
				if err != nil {
					c.ErrorLogOnly("Encountered send error", err)
					return
				}
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}

			err = c.SignalSessionDone(nil)
			if err != nil {
				c.ErrorLogOnly("Encountered send error", err)
				return
			}
		}

		// Receive next incoming client command.
		rawReq, err = c.Receive()
		if err != nil {
			c.ErrorLogOnly("Encountered receive error", err)
			return
		}
	}

	log.Println("DISTRIBUTOR sent '> done <'")
}
