package comm

import (
	"fmt"
	"strconv"
	"strings"

	"encoding/base64"
)

// Structs

// Message represents a CRDT synchronization message
// between nodes in a pluto system. It consists of the
// vector clock of the originating node and a CRDT payload
// to apply at receiver's CRDT replica.
type Message struct {
	Sender  string
	VClock  map[string]int
	Payload string
}

type Element struct {
	Value    string
	Tag      string
	Contents string
}

// Mailbox messages.

type CreateMsg struct {
	User       string
	AddMailbox *Element
}

type DeleteMsg struct {
	User       string
	RmvMailbox []*Element
}

type RenameMsg struct {
	User       string
	Mailbox    string
	RmvMailbox []*Element
	AddMailbox *Element
	AddMails   []*Element
}

// Mail messages.

type AppendMsg struct {
	User    string
	Mailbox string
	AddMail *Element
}

type ExpungeMsg struct {
	User     string
	Mailbox  string
	RmvMails []*Element
}

type StoreMsg struct {
	User    string
	Mailbox string
	RmvMail []*Element
	AddMail *Element
}

type CopyMsg struct {
	User     string
	Mailbox  string
	AddMails []*Element
}

// Functions

// InitMessage returns a fresh Message variable.
func InitMessage() *Message {

	return &Message{
		VClock: make(map[string]int),
	}
}

// String marshalls given Message m into string representation
// so that we can send it out onto the TLS connection.
func (m *Message) String() string {

	var vclockValues string

	// Merge together all vector clock entries.
	for id, value := range m.VClock {

		if vclockValues == "" {
			vclockValues = fmt.Sprintf("%s:%d", id, value)
		} else {
			vclockValues = fmt.Sprintf("%s;%s:%d", vclockValues, id, value)
		}
	}

	// Return final string representation.
	return fmt.Sprintf("%s|%s|%s", m.Sender, vclockValues, m.Payload)
}

// Parse takes in supplied string representing a received
// message and parses it back into message struct form.
func Parse(msg string) (*Message, error) {

	// Initialize new message struct.
	m := InitMessage()

	// Remove attached newline symbol.
	msg = strings.TrimRight(msg, "\n")

	// Split message at pipe symbol at maximum two times.
	tmpMsg := strings.SplitN(msg, "|", 3)

	// Messages with less than three parts are discarded.
	if len(tmpMsg) < 3 {
		return nil, fmt.Errorf("invalid sync message")
	}

	// Check sender part of message.
	if len(tmpMsg[0]) < 1 {
		return nil, fmt.Errorf("invalid sync message because sender node name is missing")
	}

	// Put sender name into struct.
	m.Sender = tmpMsg[0]

	// Split first part at semicolons for vector clock.
	tmpVClock := strings.Split(tmpMsg[1], ";")

	if len(tmpVClock) < 2 {

		// Split at colon.
		c := strings.Split(tmpVClock[0], ":")

		// Vector clock entries with less than two parts are discarded.
		if len(c) < 2 {
			return nil, fmt.Errorf("invalid vector clock element")
		}

		// Parse number from string.
		num, err := strconv.Atoi(c[1])
		if err != nil {
			return nil, fmt.Errorf("invalid number as element in vector clock")
		}

		// Place vector clock entry in struct.
		m.VClock[c[0]] = num
	} else {

		// Range over all vector clock entries.
		for _, pair := range tmpVClock {

			// Split at colon.
			c := strings.Split(pair, ":")

			// Vector clock entries with less than two parts are discarded.
			if len(c) < 2 {
				return nil, fmt.Errorf("invalid vector clock element")
			}

			// Parse number from string.
			num, err := strconv.Atoi(c[1])
			if err != nil {
				return nil, fmt.Errorf("invalid number as element in vector clock")
			}

			// Place vector clock entries in struct.
			m.VClock[c[0]] = num
		}
	}

	// Put message payload into struct.
	m.Payload = tmpMsg[2]

	// Initialize new message struct with parsed values.
	return m, nil
}

// ParseOp takes in raw incoming message payload, parses
// out the operation and returns it with the remaining
// part of the payload.
func ParseOp(payload string) (string, string, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.SplitN(payload, "|", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("payload invalid because it contained not enough pipe symbols")
	}

	return parts[0], parts[1], nil
}

func ParseCreate(payload string) (*CreateMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid CREATE message: incorrect amount of pipe symbols")
	}

	// Split element at delimiter (semicolon).
	element := strings.Split(parts[1], ";")
	if len(element) != 2 {
		return nil, fmt.Errorf("invalid element in CREATE message: incorrect amount of semicola")
	}

	// Decode value part of message encoded in base64.
	decValue, err := base64.StdEncoding.DecodeString(element[0])
	if err != nil {
		return nil, fmt.Errorf("decoding base64 value of CREATE message failed: %s\n", err.Error())
	}

	return &CreateMsg{
		User: parts[0],
		AddMailbox: &Element{
			Value: string(decValue),
			Tag:   element[1],
		},
	}, nil
}

func ParseDelete(payload string) (*DeleteMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid DELETE message: incorrect amount of pipe symbols")
	}

	// Split elements at delimiter (semicolon).
	elements := strings.Split(parts[1], ";")
	if len(elements) < 2 {
		return nil, fmt.Errorf("invalid elements in DELETE message: too few semicola")
	}

	// We expect an even number of arguments:
	// v1;t1;v2;t2;v3;t3;...
	if (len(elements) % 2) != 0 {
		return nil, fmt.Errorf("invalid elements in DELETE message: odd number of elements")
	}

	// Initialize slice of *Element of correct size.
	mailbox := make([]*Element, (len(elements) / 2))
	i := 0

	// Range over all received value-tag-pairs.
	for value := 0; value < len(elements); value += 2 {

		tag := value + 1

		// Decode value part of message encoded in base64.
		decValue, err := base64.StdEncoding.DecodeString(elements[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 value of DELETE message failed: %s\n", err.Error())
		}

		// Create a new Element of current pair.
		element := &Element{
			Value: string(decValue),
			Tag:   elements[tag],
		}

		// Insert it into mailbox slice.
		mailbox[i] = element
		i++
	}

	return &DeleteMsg{
		User:       parts[0],
		RmvMailbox: mailbox,
	}, nil
}

func ParseRename(payload string) (*RenameMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid RENAME message: incorrect amount of pipe symbols")
	}

	// Split elements of mailbox to remove at
	// delimiter (semicolon).
	elements := strings.Split(parts[2], ";")
	if len(elements) < 2 {
		return nil, fmt.Errorf("invalid elements of mailbox to remove in RENAME message: too few semicola")
	}

	// We expect an even number of arguments:
	// v1;t1;v2;t2;v3;t3;...
	if (len(elements) % 2) != 0 {
		return nil, fmt.Errorf("invalid elements of mailbox to remove in RENAME message: odd number of elements")
	}

	// Initialize slice of *Element of correct size.
	rmvMailbox := make([]*Element, (len(elements) / 2))
	i := 0

	// Range over all received value-tag-pairs.
	for value := 0; value < len(elements); value += 2 {

		tag := value + 1

		// Decode value part of message encoded in base64.
		decValue, err := base64.StdEncoding.DecodeString(elements[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 value of RENAME message failed: %s\n", err.Error())
		}

		// Create a new Element of current pair.
		element := &Element{
			Value: string(decValue),
			Tag:   elements[tag],
		}

		// Insert it into rmvMailbox slice.
		rmvMailbox[i] = element
		i++
	}

	// Split element for mailbox to add at
	// delimiter (semicolon).
	element := strings.Split(parts[3], ";")
	if len(element) != 2 {
		return nil, fmt.Errorf("invalid element for mailbox to add in RENAME message: incorrect amount of semicola")
	}

	// Decode value part of message encoded in base64.
	decAddValue, err := base64.StdEncoding.DecodeString(element[0])
	if err != nil {
		return nil, fmt.Errorf("decoding base64 value of RENAME message failed: %s\n", err.Error())
	}

	// Split elements of mails to add to new
	// mailbox at delimiter (semicolon).
	elements = strings.Split(parts[4], ";")

	// We expect a number of arguments divisible by 3:
	// v1;t1;c1;v2;t2;c2;v3;t3;c3;...
	if (len(elements) % 3) != 0 {
		return nil, fmt.Errorf("invalid elements of mails to add to renamed mailbox in RENAME message: number not divisible by 3")
	}

	// Initialize slice of *Element of correct size.
	addMails := make([]*Element, (len(elements) / 3))
	i = 0

	// Range over all received value-tag-contents-pairs.
	for value := 0; value < len(elements); value += 3 {

		tag := value + 1
		contents := value + 2

		// Decode value part of message encoded in base64.
		decValue, err := base64.StdEncoding.DecodeString(elements[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 value of RENAME message failed: %s\n", err.Error())
		}

		// Create a new Element of current pair.
		element := &Element{
			Value:    string(decValue),
			Tag:      elements[tag],
			Contents: elements[contents],
		}

		// Insert it into addMails slice.
		addMails[i] = element
		i++
	}

	return &RenameMsg{
		User:       parts[0],
		Mailbox:    parts[1],
		RmvMailbox: rmvMailbox,
		AddMailbox: &Element{
			Value: string(decAddValue),
			Tag:   element[1],
		},
		AddMails: addMails,
	}, nil
}

func ParseAppend(payload string) (*AppendMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid APPEND message: incorrect amount of pipe symbols")
	}

	// Split element at delimiter (semicolon).
	element := strings.Split(parts[2], ";")
	if len(element) != 3 {
		return nil, fmt.Errorf("invalid element of message to add in APPEND message: incorrect amount of semicola")
	}

	// Decode value part of message encoded in base64.
	decValue, err := base64.StdEncoding.DecodeString(element[0])
	if err != nil {
		return nil, fmt.Errorf("decoding base64 value of APPEND message failed: %s\n", err.Error())
	}

	return &AppendMsg{
		User:    parts[0],
		Mailbox: parts[1],
		AddMail: &Element{
			Value:    string(decValue),
			Tag:      element[1],
			Contents: element[2],
		},
	}, nil
}

func ParseExpunge(payload string) (*ExpungeMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid EXPUNGE message: incorrect amount of pipe symbols")
	}

	// Split elements at delimiter (semicolon).
	elements := strings.Split(parts[2], ";")
	if len(elements) < 2 {
		return nil, fmt.Errorf("invalid elements of mails to remove in EXPUNGE message: too few semicola")
	}

	// We expect an even number of arguments:
	// v1;t1;v2;t2;v3;t3;...
	if (len(elements) % 2) != 0 {
		return nil, fmt.Errorf("invalid elements of mails to remove in EXPUNGE message: odd number of elements")
	}

	// Initialize slice of *Element of correct size.
	mails := make([]*Element, (len(elements) / 2))
	i := 0

	// Range over all received value-tag-pairs.
	for value := 0; value < len(elements); value += 2 {

		tag := value + 1

		// Decode value part of message encoded in base64.
		decValue, err := base64.StdEncoding.DecodeString(elements[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 value of EXPUNGE message failed: %s\n", err.Error())
		}

		// Create a new Element of current pair.
		element := &Element{
			Value: string(decValue),
			Tag:   elements[tag],
		}

		// Insert it into mails slice.
		mails[i] = element
		i++
	}

	return &ExpungeMsg{
		User:     parts[0],
		Mailbox:  parts[1],
		RmvMails: mails,
	}, nil
}

func ParseStore(payload string) (*StoreMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid STORE message: incorrect amount of pipe symbols")
	}

	// Split elements of mails to remove at
	// delimiter (semicolon).
	elements := strings.Split(parts[2], ";")
	if len(elements) < 2 {
		return nil, fmt.Errorf("invalid elements of mails to remove in STORE message: too few semicola")
	}

	// We expect an even number of arguments:
	// v1;t1;v2;t2;v3;t3;...
	if (len(elements) % 2) != 0 {
		return nil, fmt.Errorf("invalid elements of mails to remove in STORE message: odd number of elements")
	}

	// Initialize slice of *Element of correct size.
	rmvMails := make([]*Element, (len(elements) / 2))
	i := 0

	// Range over all received value-tag-pairs.
	for value := 0; value < len(elements); value += 2 {

		tag := value + 1

		// Decode value part of message encoded in base64.
		decValue, err := base64.StdEncoding.DecodeString(elements[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 value of STORE message failed: %s\n", err.Error())
		}

		// Create a new Element of current pair.
		element := &Element{
			Value: string(decValue),
			Tag:   elements[tag],
		}

		// Insert it into rmvMails slice.
		rmvMails[i] = element
		i++
	}

	// Split element of mail to be renamed to at
	// delimiter (semicolon).
	element := strings.Split(parts[3], ";")
	if len(element) != 3 {
		return nil, fmt.Errorf("invalid element of renamed mail in STORE message: incorrect amount of semicola")
	}

	// Decode value part of message encoded in base64.
	decAddValue, err := base64.StdEncoding.DecodeString(element[0])
	if err != nil {
		return nil, fmt.Errorf("decoding base64 value of STORE message failed: %s\n", err.Error())
	}

	return &StoreMsg{
		User:    parts[0],
		Mailbox: parts[1],
		RmvMail: rmvMails,
		AddMail: &Element{
			Value:    string(decAddValue),
			Tag:      element[1],
			Contents: element[2],
		},
	}, nil
}

func ParseCopy(payload string) (*CopyMsg, error) {

	// Split payload at delimiter (pipe symbol).
	parts := strings.Split(payload, "|")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid COPY message: incorrect amount of pipe symbols")
	}

	// Split elements of mails to copy at
	// delimiter (semicolon).
	elements := strings.Split(parts[2], ";")

	// We expect a number of arguments divisible by 3:
	// v1;t1;c1;v2;t2;c2;v3;t3;c3;...
	if (len(elements) % 3) != 0 {
		return nil, fmt.Errorf("invalid elements of mails to copy in COPY message: number not divisible by 3")
	}

	// Initialize slice of *Element of correct size.
	addMails := make([]*Element, (len(elements) / 3))
	i := 0

	// Range over all received value-tag-contents-pairs.
	for value := 0; value < len(elements); value += 3 {

		tag := value + 1
		contents := value + 2

		// Decode value part of message encoded in base64.
		decValue, err := base64.StdEncoding.DecodeString(elements[value])
		if err != nil {
			return nil, fmt.Errorf("decoding base64 value of COPY message failed: %s\n", err.Error())
		}

		// Create a new Element of current pair.
		element := &Element{
			Value:    string(decValue),
			Tag:      elements[tag],
			Contents: elements[contents],
		}

		// Insert it into addMails slice.
		addMails[i] = element
		i++
	}

	return &CopyMsg{
		User:     parts[0],
		Mailbox:  parts[1],
		AddMails: addMails,
	}, nil
}
