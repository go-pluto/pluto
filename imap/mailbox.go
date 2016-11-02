package imap

import (
	"github.com/numbleroot/pluto/conn"
)

// Functions

// AcceptMailbox acts as the main loop for requests
// targeted at the IMAP mailbox state. It parses
// incoming requests and executes command specific
// handlers matching the parsed data.
func (node *Node) AcceptMailbox(c *conn.Connection) {

	// TODO: Implement this function.
}
