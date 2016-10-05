package imap

import (
	"log"
)

// Functions

// AcceptNotAuthenticated acts as the main loop for
// requests targeted at the IMAP not authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (c *Connection) AcceptNotAuthenticated() {

	log.Println("[imap.AcceptNotAuthenticated] Received call.")

	// TODO: Refactor endless loop into loop with clear end conditions.
	for {

		// Receive incoming client command.
		rawReq, err := c.Receive()
		if err != nil {
			log.Fatal(err)
		}

		// Parse received raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("[imap.AcceptNotAuthenticated] req.Tag: '%s' - req.Command: '%s' - req.Payload: '%s'\n", req.Tag, req.Command, req.Payload)
	}
}
