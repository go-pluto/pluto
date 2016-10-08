package imap

import (
	"log"
)

// Functions

// AcceptAuthenticated acts as the main loop for
// requests targeted at the IMAP authenticated state.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (c *Connection) AcceptAuthenticated() {

	// TODO: Implement this function.

	log.Println("[imap.AcceptAuthenticated] Received call.")
}
