package comm

import (
	"fmt"
)

// Structs

// Message represents a CRDT synchronization message
// between nodes in a pluto system. It consists of the
// vector clock of the originating node and a CRDT payload
// to apply at receiver's CRDT replica.
type Message struct {
	vclock  map[string]int
	payload string
}

// Functions

// String marshalls given Message m into string representation
// so that we can send it out onto the TLS connection.
func (m Message) String() string {

	var vclockValues string

	// Merge together all vector clock entries.
	// TODO: Escape possibly contained delimiter characters.
	for id, value := range m.vclock {

		if vclockValues == "" {
			vclockValues = fmt.Sprintf("%s:%d", id, value)
		} else {
			vclockValues = fmt.Sprintf("%s;%s:%d", vclockValues, id, value)
		}
	}

	// Return final string representation.
	return fmt.Sprintf("%s|%s", vclockValues, m.payload)
}
