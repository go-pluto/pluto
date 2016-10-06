package imap

import (
	"fmt"
	"strings"
)

// Structs

// Request represents the parsed content of a client
// command line sent to pluto. Payload will be examined
// further in command specific functions.
type Request struct {
	Tag     string
	Command string
	Payload string
}

// Functions

// ParseRequest takes in a raw string representing
// a received IMAP request and parses it into the
// defined request structure above. Any error encountered
// is handled useful to the IMAP protocol.
func ParseRequest(req string) (*Request, error) {

	// Split req at space symbols at maximum two times.
	tmpReq := strings.SplitN(req, " ", 3)

	// There exists no first class IMAP command which
	// is not tag prefixed. Return an error if only one
	// token was found.
	if len(tmpReq) < 2 {
		return nil, fmt.Errorf("* BAD Received invalid IMAP command")
	}

	// Assign corresponding parts in new struct.
	finalReq := &Request{
		Tag:     tmpReq[0],
		Command: tmpReq[1],
	}

	// If the command has a defined payload, add
	// it to the struct as blob payload text.
	if len(tmpReq) > 2 {
		finalReq.Payload = tmpReq[2]
	}

	return finalReq, nil
}
