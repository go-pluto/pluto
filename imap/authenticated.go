package imap

import (
	"fmt"
)

// Functions

// Select sets mailbox based on supplied payload to
// current context.
func (node *Node) Select(c *Connection, req *Request) (success bool) {

	// TODO: Implement this function.

	return true
}

// AcceptAuthenticated acts as the main loop for
// requests targeted at the IMAP authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (node *Node) AcceptAuthenticated(c *Connection) {

	// fmt.Println("Incoming AUTHENTICATED!")

	// Set loop end condition initially to this state.
	nextState := AUTHENTICATED

	// As long as no transition to next consecutive IMAP state
	// took place, wait in loop for incoming requests.
	for nextState == AUTHENTICATED {

		// Receive incoming client command.
		rawReq, err := c.Receive()
		if err != nil {
			node.Error(c, "Encountered receive error", err)
			return
		}

		// Parse received raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				node.Error(c, "Encountered send error", err)
				return
			}

			// Go back to beginning of for loop.
			continue
		}

		switch req.Command {

		case "CAPABILITY":
			node.Capability(c, req)

		case "LOGIN":
			node.Login(c, req)

		case "LOGOUT":
			if ok := node.Logout(c, req); ok {
				// After an LOGOUT, return to LOGOUT state.
				nextState = LOGOUT
			}

		case "SELECT":
			if ok := node.Select(c, req); ok {
				// If SELECT was successful, we switch
				// to MAILBOX state.
				nextState = MAILBOX
			}

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				node.Error(c, "Encountered send error", err)
				return
			}
		}
	}

	switch nextState {

	case MAILBOX:
		c.Transition(node, MAILBOX)

	case LOGOUT:
		return
	}
}
