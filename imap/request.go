package imap

import (
	"fmt"
	"strings"
)

// Variables

// SupportedCommands is a quick access map
// for checking if a supplied IMAP command
// is supported by pluto.
var SupportedCommands map[string]bool

// Structs

// Request represents the parsed content of a client
// command line sent to pluto. Payload will be examined
// further in command specific functions.
type Request struct {
	Tag     string
	Command string
	Payload string
}

// Context carries session-identifying information sent
// from distributor to worker node.
type Context struct {
	UserToken   string
	UserName    string
	UserMaildir string
	UserCRDT    string
}

// Functions

func init() {

	// Set supported IMAP commands to true in
	// a map to have quick access.
	SupportedCommands = make(map[string]bool)

	SupportedCommands["STARTTLS"] = true
	SupportedCommands["LOGIN"] = true
	SupportedCommands["CAPABILITY"] = true
	SupportedCommands["LOGOUT"] = true
	SupportedCommands["SELECT"] = true
	SupportedCommands["CREATE"] = true
	SupportedCommands["APPEND"] = true
	SupportedCommands["STORE"] = true
	SupportedCommands["COPY"] = true
	SupportedCommands["EXPUNGE"] = true
}

// ParseRequest takes in a raw string representing
// a received IMAP request and parses it into the
// defined request structure above. Any error encountered
// is handled useful to the IMAP protocol.
func ParseRequest(req string) (*Request, error) {

	// Split req at space symbols at maximum two times.
	tmpReq := strings.SplitN(req, " ", 3)

	// There exists no first class IMAP command with less
	// than two arguments. Return an error if only one
	// token was found.
	if len(tmpReq) < 2 {
		return nil, fmt.Errorf("* BAD Received invalid IMAP command")
	}

	// Check that the tag was not left out.
	if SupportedCommands[tmpReq[0]] {
		return nil, fmt.Errorf("* BAD Received invalid IMAP command")
	}

	// Assign corresponding parts in new struct.
	finalReq := &Request{
		Tag:     tmpReq[0],
		Command: strings.ToUpper(tmpReq[1]),
	}

	// If the command has a defined payload, add
	// it to the struct as blob payload text.
	if len(tmpReq) > 2 {
		finalReq.Payload = tmpReq[2]
	}

	return finalReq, nil
}

// ExtractContext takes in received raw context string,
// verifies and parses it and if successful, returns
// context information about session.
func ExtractContext(contextRaw string) (*Context, error) {

	// Split received context at white spaces and check
	// for correct amount of found fields.
	contexts := strings.Fields(contextRaw)
	if len(contexts) != 6 {
		return nil, fmt.Errorf("received an invalid context information")
	}

	// Check if structure is correct.
	if contexts[0] != ">" || contexts[1] != "token:" || contexts[3] != "name:" || contexts[5] != "<" {
		return nil, fmt.Errorf("received an invalid context information")
	}

	// Extract token and name of client and store it
	// in connection context.
	return &Context{
		UserToken: contexts[2],
		UserName:  contexts[4],
	}, nil
}
