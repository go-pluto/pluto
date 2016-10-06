package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

// Constants

const (
	// Integer counter for IMAP states.
	ANY IMAPState = iota
	NOT_AUTHENTICATED
	AUTHENTICATED
	MAILBOX
	LOGOUT
)

// Structs

// IMAPState represents the integer value associated
// with one of the implemented IMAP states a connection
// can be in.
type IMAPState int

// Connection carries all information specific
// to one observed connection on its way through
// the IMAP server.
type Connection struct {
	Conn   net.Conn
	Reader *bufio.Reader
	State  IMAPState
}

// Functions

// NewConnection creates a new element of above
// connection struct and fills it with content from
// a supplied, real IMAP connection.
func NewConnection(c net.Conn) *Connection {

	return &Connection{
		Conn:   c,
		Reader: bufio.NewReader(c),
	}
}

// Transition is the broker between the IMAP states.
// It is called to switch from one IMAP state to the
// consecutive following one as instructed by received
// IMAP commands.
func (c *Connection) Transition(state IMAPState) {

	switch state {

	case NOT_AUTHENTICATED:
		log.Println("[imap.DEBUG] NOT_AUTHENTICATED chosen.")
		c.State = NOT_AUTHENTICATED
		go c.AcceptNotAuthenticated()

	case AUTHENTICATED:
		log.Println("[imap.DEBUG] AUTHENTICATED chosen.")
		c.State = AUTHENTICATED
		go c.AcceptAuthenticated()

	case MAILBOX:
		log.Println("[imap.DEBUG] MAILBOX chosen.")
		c.State = MAILBOX
		go c.AcceptMailbox()

	case LOGOUT:
		log.Println("[imap.DEBUG] LOGOUT chosen.")
		c.State = LOGOUT
		go c.AcceptLogout()
	}
}

// Receive wraps the main io.Reader function that
// awaits text until a newline symbol and deletes
// that symbol afterwards again. It returns the
// resulting string or an error.
func (c *Connection) Receive() (string, error) {

	text, err := c.Reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\n"), nil
}

// Send takes in an answer text from server as a
// string and writes it to the connection to the client.
// In case an error occurs, this method returns it to
// the calling function.
func (c *Connection) Send(text string) error {

	if _, err := fmt.Fprintf(c.Conn, "%s\n", text); err != nil {
		return err
	}

	return nil
}

// Terminate tears down the state of a connection.
// This includes closing contained connection items.
// It returns nil or eventual errors.
func (c *Connection) Terminate() error {

	if err := c.Conn.Close(); err != nil {
		return err
	}

	return nil
}

// Error makes use of Terminate but provides an additional
// error message before terminating.
func (c *Connection) Error(msg string, err error) {

	// Log error.
	log.Printf("%s: %s. Terminating connection to %s\n", msg, err.Error(), c.Conn.RemoteAddr().String())

	// Terminate connection.
	err = c.Terminate()
	if err != nil {
		log.Fatal(err)
	}
}
