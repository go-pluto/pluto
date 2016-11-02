package imap

import (
	"fmt"

	"github.com/numbleroot/pluto/conn"
)

// Functions

// Select sets mailbox based on supplied payload to
// current context.
func (node *Node) Select(c *conn.Connection, req *Request) (success bool) {

	// TODO: Implement this function.

	return true
}

// AcceptAuthenticated acts as the main loop for
// requests targeted at the IMAP authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (node *Node) AcceptAuthenticated(c *conn.Connection) {

	// Handle traffic as write-through if responsible
	// worker field is not empty.
	if c.Worker != nil {

		err := node.Proxy(c)
		if err != nil {
			c.Error("AUTHENTICATED Proxy mode error", err)
			return
		}

		return
	}

	// Set loop end condition initially to this state.
	nextState := conn.AUTHENTICATED

	// As long as no transition to next consecutive IMAP state
	// took place, wait in loop for incoming requests.
	for nextState == conn.AUTHENTICATED {

		// Receive incoming client command.
		rawReq, err := c.Receive()
		if err != nil {
			c.Error("Encountered receive error", err)
			return
		}

		// Parse received raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			// Go back to beginning of for loop.
			continue
		}

		switch req.Command {

		case "CAPABILITY":
			ok := node.Capability(c, req)

			// If successful and this node is a worker,
			// signal end of current operation to distributor.
			if ok && node.Type == WORKER {

				err := c.SignalWorkerDone()
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case "LOGIN":
			ok := node.Login(c, req)

			// If successful and this node is a worker,
			// signal end of current operation to distributor.
			if ok && node.Type == WORKER {

				err := c.SignalWorkerDone()
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}
			}

		case "LOGOUT":
			ok := node.Logout(c, req)

			// If successful and this node is a worker,
			// signal end of current operation to distributor.
			if ok && node.Type == WORKER {

				err := c.SignalWorkerDone()
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}

				// After a LOGOUT, move on to LOGOUT state.
				nextState = conn.LOGOUT
			}

		case "SELECT":
			ok := node.Select(c, req)

			// If successful and this node is a worker,
			// signal end of current operation to distributor.
			if ok && node.Type == WORKER {

				err := c.SignalWorkerDone()
				if err != nil {
					c.Error("Encountered send error", err)
					return
				}

				// If SELECT was successful, we switch
				// to MAILBOX state.
				nextState = conn.MAILBOX
			}

		default:

			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			// Signal end of current operation to distributor.
			err = c.SignalWorkerDone()
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}

	switch nextState {

	case conn.MAILBOX:
		node.Transition(c, conn.MAILBOX)

	case conn.LOGOUT:
		return
	}
}
