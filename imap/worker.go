package imap

import (
	"fmt"
	"log"
)

// Functions

// Select sets mailbox based on supplied payload to
// current context.
func (node *Node) Select(c *Connection, req *Request) bool {

	log.Printf("Serving SELECT '%s'...\n", req.Tag)

	// TODO: Implement this function.

	// Temporary answer.
	err := c.Send("Yep, works.")
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

		// TODO: Load user-specific environment.

		switch {

		case rawReq == "> done <":
			// TODO: Trigger state-dependent behaviour when user logged out.
			log.Printf("%s: done.", context.UserName)

		case rawReq == "> changed <":
			// TODO: Trigger state-dependent behaviour when session changed.
			log.Printf("%s: session changed.", context.UserName)

		case req.Command == "SELECT":
			if ok := node.Select(c, req); ok {

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
