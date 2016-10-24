package imap

import (
	"fmt"
	"strings"
)

// Functions

// Capability handles the IMAP CAPABILITY command.
// It outputs the supported actions in the current state.
func (node *Node) Capability(c *Connection, req *Request) (success bool) {

	if len(req.Payload) > 0 {

		// If payload was not empty to CAPABILITY command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command CAPABILITY was sent with extra parameters", req.Tag))
		if err != nil {
			node.Error(c, "Encountered send error", err)
			return false
		}

		return false
	}

	// Send mandatory capability options.
	// This means, AUTH=PLAIN is allowed and nothing else.
	// STARTTLS will be answered but is not listed as
	// each connection already is a TLS connection.
	err := c.Send(fmt.Sprintf("* CAPABILITY IMAP4rev1 AUTH=PLAIN\n%s OK CAPABILITY completed", req.Tag))
	if err != nil {
		node.Error(c, "Encountered send error", err)
		return false
	}

	return true
}

// Login performs the authentication mechanism specified
// as part of the distributor config.
func (node *Node) Login(c *Connection, req *Request) (success bool) {

	// Split payload on every space character.
	userCredentials := strings.Split(req.Payload, " ")

	if len(userCredentials) != 2 {

		// If payload did not contain exactly two elements,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGIN was not sent with exactly two parameters", req.Tag))
		if err != nil {
			node.Error(c, "Encountered send error", err)
			return false
		}

		return false
	}

	id, token, err := node.AuthAdapter.AuthenticatePlain(userCredentials[0], userCredentials[1])
	if err != nil {

		// If supplied credentials failed to authenticate client,
		// they are invalid. Return NO statement.
		err := c.Send(fmt.Sprintf("%s NO Name and / or password wrong", req.Tag))
		if err != nil {
			node.Error(c, "Encountered send error", err)
			return false
		}

		return false
	}

	// Set connection elements concerning user
	// to values retrieved above.
	c.UserID = id
	c.UserToken = token

	// Signal success to client.
	err = c.Send(fmt.Sprintf("%s OK Logged in", req.Tag))
	if err != nil {
		node.Error(c, "Encountered send error", err)
		return false
	}

	return true
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (node *Node) Logout(c *Connection, req *Request) (success bool) {

	if len(req.Payload) > 0 {

		// If payload was not empty to LOGOUT command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGOUT was sent with extra parameters", req.Tag))
		if err != nil {
			node.Error(c, "Encountered send error", err)
			return false
		}

		return false
	}

	// Signal success to client.
	err := c.Send(fmt.Sprintf("* BYE Terminating connection\n%s OK LOGOUT completed", req.Tag))
	if err != nil {
		node.Error(c, "Encountered send error", err)
		return false
	}

	// Terminate connection.
	node.Terminate(c)

	return true
}
