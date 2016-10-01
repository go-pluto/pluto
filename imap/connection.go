package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

// Structs

// Connection carries all information specific
// to one observed connection on its way through
// the IMAP server.
type Connection struct {
	Conn   net.Conn
	Reader *bufio.Reader
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

// Receive wraps the main io.Reader function that
// awaits text until a newline symbol and deletes
// that symbol afterwards again. It returns the
// resulting string.
func (c *Connection) Receive() string {

	text, err := c.Reader.ReadString('\n')
	if err != nil {
		log.Fatalf("[imap.Receive] Fatal error while reading in commands from IMAP client: %s\n", err.Error())
	}

	return strings.TrimRight(text, "\n")
}

// Send takes in an answer text from server as a
// string and writes it to the connection to the client.
// In case an error occurs, this method logs the error
// and exits with a failure code.
func (c *Connection) Send(text string) {

	if _, err := fmt.Fprintf(c.Conn, "%s\n", text); err != nil {
		log.Fatalf("[imap.Send] Fatal error occured during write of text to IMAP client: %s\n", err.Error())
	}
}
