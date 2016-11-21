package comm

import (
	"fmt"
	"strconv"
	"strings"
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

// Parse takes in supplied string representing a received
// message and parses it back into message struct form.
func Parse(msg string) (*Message, error) {

	// Initialize new message struct.
	m := &Message{
		vclock: make(map[string]int),
	}

	// Remove attached newline symbol.
	msg = strings.TrimRight(msg, "\n")

	// Split message at pipe symbol at maximum one time.
	tmpMsg := strings.SplitN(msg, "|", 2)

	// Messages with less than two parts are discarded.
	if len(tmpMsg) < 2 {
		return nil, fmt.Errorf("Invalid sync message")
	}

	// Split first part at semicolons for vector clock.
	tmpVClock := strings.Split(tmpMsg[0], ";")

	if len(tmpVClock) < 2 {

		// Split at colon.
		c := strings.Split(tmpVClock[0], ":")

		// Vector clock entries with less than two parts are discarded.
		if len(c) < 2 {
			return nil, fmt.Errorf("Invalid vector clock element")
		}

		// Parse number from string.
		num, err := strconv.Atoi(c[1])
		if err != nil {
			return nil, fmt.Errorf("Invalid number as element in vector clock")
		}

		// Place vector clock entry in struct.
		m.vclock[c[0]] = num
	} else {

		// Range over all vector clock entries.
		for _, pair := range tmpVClock {

			// Split at colon.
			c := strings.Split(pair, ":")

			// Vector clock entries with less than two parts are discarded.
			if len(c) < 2 {
				return nil, fmt.Errorf("Invalid vector clock element")
			}

			// Parse number from string.
			num, err := strconv.Atoi(c[1])
			if err != nil {
				return nil, fmt.Errorf("Invalid number as element in vector clock")
			}

			// Place vector clock entries in struct.
			m.vclock[c[0]] = num
		}
	}

	// Put message payload into struct.
	m.payload = tmpMsg[1]

	// Initialize new message struct with parsed values.
	return m, nil
}
