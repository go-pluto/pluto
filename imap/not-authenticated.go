package imap

import (
	"fmt"
)

// Functions

// StartTLS states on IMAP STARTTLS command
// that current connection is already encrypted.
func (c *Connection) StartTLS(req *Request) {

	if len(req.Payload) > 0 {

		// If payload was not empty to STARTTLS command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command STARTTLS was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		return
	}

	// As the connection is already TLS encrypted,
	// tell client that a TLS session is active.
	err := c.Send(fmt.Sprintf("%s BAD TLS is already active", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return
	}
}

// Authenticate parses included base64 encoded user name
// and password and tries to authenticate them against the
// server's defined user database.
func (c *Connection) AuthenticatePlain(req *Request) {

	// TODO: Implement this functionality.
}

// AcceptNotAuthenticated acts as the main loop for
// requests targeted at the IMAP not authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (c *Connection) AcceptNotAuthenticated() {

	// Set loop end condition initially to this state.
	nextState := NOT_AUTHENTICATED

	// As long as no transition to next consecutive IMAP state
	// took place, wait in loop for incoming requests.
	for nextState == NOT_AUTHENTICATED {

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
			c.Capability(req)

		case "LOGIN":
			c.Login(req)

		case "LOGOUT":
			c.Logout(req)
			nextState = LOGOUT

		case "STARTTLS":
			c.StartTLS(req)

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
	}
}
