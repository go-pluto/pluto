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
		err := c.Send("* BAD Command CAPABILITY was sent with extra space")
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
		err := c.Send(fmt.Sprintf("* CAPABILITY IMAP4rev1 AUTH=PLAIN\n%s OK CAPABILITY completed", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return
		}

		return
	}
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (c *Connection) Logout(req *Request) {

	// TODO: Implement this function.
}
