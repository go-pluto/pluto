package imap

import (
	"fmt"
	"log"
	"strings"
)

// Functions

// Select sets mailbox based on supplied payload to
// current context.
func (node *Node) Select(c *Connection, req *Request, ctx *Context) bool {

	log.Printf("Serving SELECT '%s'...\n", req.Tag)

	// Check if connection is in correct state.
	if (c.IMAPState == ANY) || (c.IMAPState == NOT_AUTHENTICATED) || (c.IMAPState == LOGOUT) {
		log.Printf("SELECT not correct state lol")
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

		return false
	}

	// Split payload on every space character.
	mailboxes := strings.Split(req.Payload, " ")

	if len(mailboxes) != 1 {

		// If there were more than two names supplied to select,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command SELECT was sent with multiple mailbox names instead of only one", req.Tag))
		if err != nil {
			c.ErrorLogOnly("Encountered send error", err)
			return false
		}

		return false
	}

	// Save selected mailbox.
	mailbox := mailboxes[0]
	log.Printf("selected mailbox: %s\n", mailbox)

	// TODO: Check if mailbox exists as folder.

	// TODO: Check if mailbox is a conformant maildir folder.

	// TODO: Set selected mailbox in connection struct to supplied
	//       one and advance IMAP state of connection to MAILBOX.

	// Build up answer to client.
	answer := ""

	// Include part for standard flags.
	answer = answer + "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)"

	// TODO: Add all other required answer parts.

	log.Printf("Answer: '%s'\n", answer)

	// Send prepared answer to requesting client.
	err := c.Send(answer)
	if err != nil {
		c.ErrorLogOnly("Encountered send error", err)
		return false
	}

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
		context.UserMaildir = node.Config.Workers[node.Name].MaildirRoot + context.UserName + "/"
		context.UserCRDT = node.Config.Workers[node.Name].CRDTLayerRoot + context.UserName + "/"

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
