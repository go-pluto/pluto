package imap

import (
	"fmt"
	"log"
)

// Functions

func (c *Connection) StartTLS(req *Request) {

	// TODO: Implement this function.
}

// AcceptNotAuthenticated acts as the main loop for
// requests targeted at the IMAP not authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (c *Connection) AcceptNotAuthenticated() {

	var nextState IMAPState

	// As long as no transition to next consecutive IMAP state
	// took place, wait in loop for incoming requests.
	for (nextState != AUTHENTICATED) || (nextState != MAILBOX) {

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

		log.Printf("tag: '%s', command: '%s', payload: '%s'\n", req.Tag, req.Command, req.Payload)

		switch req.Command {

		case "STARTTLS":
			c.StartTLS(req)
			nextState = AUTHENTICATED

		case "LOGIN":
			c.Login(req)

		case "CAPABILITY":
			c.Capability(req)

		case "LOGOUT":
			c.Logout(req)
			nextState = LOGOUT

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}

	switch nextState {

	case AUTHENTICATED:
		c.Transition(AUTHENTICATED)

	case LOGOUT:
		c.Transition(LOGOUT)
	}
}
