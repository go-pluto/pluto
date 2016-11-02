package imap

import (
	"fmt"
	"log"
)

// Functions

// Select sets mailbox based on supplied payload to
// current context.
func (node *Node) Select(c *Connection, req *Request) bool {

	log.Println("SELECT served")

	// TODO: Implement this function.

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
	rawReq, err := c.Receive()
	if err != nil {
		c.Error("Encountered receive error", err)
		return
	}

	// As long as the distributor node did not indicate
	// that the client logged out, we accept handle requests.
	for rawReq != "> done <" {

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			// In case of failure, wait for next sent command.
			rawReq, err = c.Receive()
			if err != nil {
				c.Error("Encountered receive error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		switch {

		case req.Command == "SELECT":
			if ok := node.Select(c, req); ok {

				// If successful, signal end of operation to distributor.
				err := c.SignalWorkerDone()
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			err = c.SignalWorkerDone()
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}

		// Receive next incoming client command.
		rawReq, err = c.Receive()
		if err != nil {
			c.Error("Encountered receive error", err)
			return
		}
	}

	log.Println("DISTRIBUTOR sent '> done <'")
}
