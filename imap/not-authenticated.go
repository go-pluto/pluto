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
			log.Fatal(err)

			// TODO: Change to clean up function and termination of
			//       connection not whole server.

			return
		}

		// Parse received raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			err := c.Send(err.Error())
			if err != nil {
				log.Fatal(err)

				// TODO: Change to clean up function and termination of
				//       connection not whole server.

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
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				log.Fatal(err)

				// TODO: Change to clean up function and termination of
				//       connection not whole server.

				return
			}
		}
	}
}
