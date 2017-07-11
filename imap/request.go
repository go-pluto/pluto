package imap

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Constants

const (
	// CommandCapability defines IMAPv4 CAPABILITY support.
	CommandCapability = "CAPABILITY"
	// CommandLogout defines IMAPv4 LOGOUT support.
	CommandLogout = "LOGOUT"
	// CommandStartTLS defines IMAPv4 STARTTLS support.
	CommandStartTLS = "STARTTLS"
	// CommandLogin defines IMAPv4 LOGIN support.
	CommandLogin = "LOGIN"
	// CommandSelect defines IMAPv4 SELECT support.
	CommandSelect = "SELECT"
	// CommandCreate defines IMAPv4 CREATE support.
	CommandCreate = "CREATE"
	// CommandDelete defines IMAPv4 DELETE support.
	CommandDelete = "DELETE"
	// CommandList defines IMAPv4 LIST support.
	CommandList = "LIST"
	// CommandAppend defines IMAPv4 APPEND support.
	CommandAppend = "APPEND"
	// CommandExpunge defines IMAPv4 EXPUNGE support.
	CommandExpunge = "EXPUNGE"
	// CommandStore defines IMAPv4 STORE support.
	CommandStore = "STORE"
)

// Variables

// SupportedCommands is a quick access map
// for checking if a supplied IMAP command
// is supported by pluto.
var SupportedCommands = map[string]bool{
	CommandCapability: true,
	CommandLogout:     true,
	CommandStartTLS:   true,
	CommandLogin:      true,
	CommandSelect:     true,
	CommandCreate:     true,
	CommandDelete:     true,
	CommandList:       true,
	CommandAppend:     true,
	CommandExpunge:    true,
	CommandStore:      true,
}

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

// ParseSeqNumbers returns complete and normalized list
// of message sequence numbers for use in e.g. STORE command.
func ParseSeqNumbers(recv string, lenMailboxContents int) ([]int, error) {

	// If supplied number of mail messages in selected
	// folder indicated an empty mailbox, immediately return.
	if lenMailboxContents == 0 {
		return nil, fmt.Errorf("Cannot select mail in empty mailbox")
	}

	// Initialize needed data stores.
	var err error
	msgNums := make([]int, 0, 6)
	seenMsgNums := make(map[int]bool)

	// Split into sequence ranges or sequence numbers.
	msgNumsSet := strings.Split(recv, ",")

	for _, numsSet := range msgNumsSet {

		numStart := 0
		numEnd := 0

		// Split into sequence numbers if not already done.
		msgNumsRange := strings.Split(numsSet, ":")

		if msgNumsRange[0] == "*" {

			// If wildcard symbol was set as beginning range
			// number, replace it with maximum number in mailbox.
			numStart = lenMailboxContents
		} else {

			// Convert string to number.
			numStart, err = strconv.Atoi(msgNumsRange[0])
			if err != nil {

				// Number parameter was invalid, client error.
				// Send tagged BAD response.
				return nil, fmt.Errorf("Command STORE was sent with an invalid number parameter")
			}
		}

		if len(msgNumsRange) > 1 {

			if msgNumsRange[1] == "*" {

				// If wildcard symbol was set as end number of range,
				// replace it with maximum number in mailbox.
				numEnd = lenMailboxContents
			} else {

				// Convert string to number.
				numEnd, err = strconv.Atoi(msgNumsRange[1])
				if err != nil {

					// Number parameter was invalid, client error.
					// Send tagged BAD response.
					return nil, fmt.Errorf("Command STORE was sent with an invalid number parameter")
				}
			}

			if numEnd < numStart {

				// If end range number is bigger than start
				// range number, exchange both values.
				numTmp := numEnd
				numEnd = numStart
				numStart = numTmp
			}
		}

		if len(msgNumsRange) == 1 {

			// Make sure that numStart refers to an
			// existing message sequence number.
			if (numStart < 1) || (numStart > lenMailboxContents) {
				return nil, fmt.Errorf("Command STORE was sent with a number parameter not referring to an existing mail message")
			}

			if _, seen := seenMsgNums[numStart]; !seen {

				// Sequence number specified, append it if
				// we have not yet seen this value.
				msgNums = append(msgNums, (numStart - 1))

				// Set corresponding seen value to true.
				seenMsgNums[numStart] = true
			}

		} else {

			// Make sure that numStart and numEnd both
			// refer to existing message sequence numbers.
			if (numStart < 1) || (numStart > lenMailboxContents) || (numEnd < 1) || (numEnd > lenMailboxContents) {
				return nil, fmt.Errorf("Command STORE was sent with a number parameter not referring to an existing mail message")
			}

			for u := numStart; u <= numEnd; u++ {

				if _, seen := seenMsgNums[u]; !seen {

					// Sequence number specified, append it if
					// we have not yet seen this value.
					msgNums = append(msgNums, (u - 1))

					// Set corresponding seen value to true.
					seenMsgNums[u] = true
				}
			}
		}
	}

	// Sort resulting numbers list.
	sort.Ints(msgNums)

	return msgNums, nil
}

// ParseFlags takes in the string representation of
// a parenthesized list of IMAP flags and returns a
// map containing all found flags.
func ParseFlags(recv string) (map[string]struct{}, error) {

	// Reserve space.
	flags := make(map[string]struct{})

	if strings.HasPrefix(recv, "(") {
		// Remove leading parenthesis.
		recv = strings.TrimPrefix(recv, "(")
	} else {
		return nil, fmt.Errorf("Command STORE was sent with invalid parenthesized flags list")
	}

	if strings.HasSuffix(recv, ")") {
		// Remove trailing parenthesis.
		recv = strings.TrimSuffix(recv, ")")
	} else {
		return nil, fmt.Errorf("Command STORE was sent with invalid parenthesized flags list")
	}

	// Split at space symbols.
	flagsRaw := strings.Split(recv, " ")

	for _, flag := range flagsRaw {
		flags[flag] = struct{}{}
	}

	return flags, nil
}
