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

	// TODO: Refactor endless loop into loop with clear end conditions.
	for {

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

		case "CAPABILITY":
			c.Capability(req)

		case "BYE":
			c.Logout(req)

		default:
			// Client sent inappropriate command. Signal error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}
}
