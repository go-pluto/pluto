package imap

import (
	"log"
)

// Functions

// AcceptStorage acts as the main loop for requests
// targeted at the storage node.
func (node *Node) AcceptStorage(c *Connection) {

	// Connections in this state are only possible against
	// a node of type STORAGE, none else.
	if node.Type != STORAGE {
		log.Println("[imap.AcceptStorage] DISTRIBUTOR or WORKER node tried to run this function. Not allowed.")
		return
	}

	// Receive next incoming client command.
	_, err := c.Receive()
	if err != nil {
		c.Error("Encountered receive error", err)
		return
	}
}
