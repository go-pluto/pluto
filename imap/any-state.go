package imap

import (
	"fmt"
)

// Functions

// Capability handles the IMAP CAPABILITY command.
// It outputs the supported actions in the current state.
func (c *Connection) Capability(req *Request) {

	if len(req.Payload) > 0 {

		// If payload was not empty to CAPABILITY command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command CAPABILITY was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		return
	}

	// Check if state is not authenticated.
	if c.State == NOT_AUTHENTICATED {

		// Send mandatory capability options in not authenticated state.
		// This means, STARTTLS is allowed and nothing else.
		err := c.Send(fmt.Sprintf("* CAPABILITY IMAP4rev1 STARTTLS LOGINDISABLED\n%s OK CAPABILITY completed", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		return
	}

	// Check if state is authenticated or mailbox.
	if (c.State == AUTHENTICATED) || (c.State == MAILBOX) {

		// Send mandatory capability options in an authenticated state.
		// This means, AUTH=PLAIN is allowed and nothing else.
		err := c.Send(fmt.Sprintf("* CAPABILITY IMAP4rev1 LOGINDISABLED AUTH=PLAIN\n%s OK CAPABILITY completed", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		return
	}
}

// Login sends NO response to any LOGIN attempt
// because LOGINDISABLED is advertised.
func (c *Connection) Login(req *Request) {

	// Prepared tagged NO response.
	response := fmt.Sprintf("%s NO Command LOGIN is disabled. Do not send plaintext login information.", req.Tag)

	err := c.Send(response)
	if err != nil {
		c.Error("Encountered send error", err)
		return
	}
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (c *Connection) Logout(req *Request) {

	if len(req.Payload) > 0 {

		// If payload was not empty to LOGOUT command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGOUT was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		return
	}

	// TODO: Implement state change in user database.

	// Signal success to client.
	err := c.Send(fmt.Sprintf("* BYE Terminating connection\n%s OK LOGOUT completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return
	}

	// TODO: Terminate connection.
	// c.Terminate()
}
