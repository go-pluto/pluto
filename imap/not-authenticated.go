package imap

import (
	"fmt"
	"log"

	"github.com/numbleroot/pluto/conn"
)

// Functions

// StartTLS states on IMAP STARTTLS command
// that current connection is already encrypted.
func (node *Node) StartTLS(c *conn.Connection, req *Request) bool {

	if len(req.Payload) > 0 {

		// If payload was not empty to STARTTLS command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command STARTTLS was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return false
	}

	// As the connection is already TLS encrypted,
	// tell client that a TLS session is active.
	err := c.Send(fmt.Sprintf("%s BAD TLS is already active", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// AcceptNotAuthenticated acts as the main loop for
// requests targeted at the IMAP not authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (node *Node) AcceptNotAuthenticated(c *conn.Connection) {

	// Connections in this state are only possible against
	// a node of type DISTRIBUTOR, none else.
	if (node.Type == WORKER) || (node.Type == STORAGE) {
		log.Println("[imap.AcceptNotAuthenticated] WORKER or STORAGE node tried to run this function. Not allowed.")
		return
	}

	// Handle traffic as write-through if responsible
	// worker field is not empty.
	if c.Worker != nil {

		err := node.Proxy(c)
		if err != nil {
			c.Error("NOT_AUTHENTICATED Proxy mode error", err)
			return
		}

		return
	}

	// Set loop end condition initially to this state.
	nextState := conn.NOT_AUTHENTICATED

	// As long as no transition to next consecutive IMAP state
	// took place, wait in loop for incoming requests.
	for nextState == conn.NOT_AUTHENTICATED {

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
			node.Capability(c, req)

		case "LOGIN":
			if ok := node.Login(c, req); ok {
				// If LOGIN was successful, we switch
				// to AUTHENTICATED state.
				nextState = conn.AUTHENTICATED
			}

		case "LOGOUT":
			if ok := node.Logout(c, req); ok {
				// After an LOGOUT, return via LOGOUT state.
				nextState = conn.LOGOUT
			}

		case "STARTTLS":
			node.StartTLS(c, req)

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

	case conn.AUTHENTICATED:
		node.Transition(c, conn.AUTHENTICATED)

	case conn.LOGOUT:
		return
	}
}
