package distributor

import (
	"bufio"
	"fmt"
	"strings"

	"crypto/tls"

	"github.com/numbleroot/pluto/imap"
)

// Structs

// Connection carries all information specific
// to one observed connection on its way through
// a pluto node that only authenticates and proxies
// IMAP connections.
type Connection struct {
	gRPCClient   imap.NodeClient
	IncConn      *tls.Conn
	IncReader    *bufio.Reader
	IsAuthorized bool
	ClientID     string
	ClientAddr   string
	UserName     string
	RespWorker   string
}

// Functions

// Send takes in an answer text from a node as a
// string and writes it to the connection to the client.
// In case an error occurs, this method returns it to
// the calling function.
func (c *Connection) Send(text string) error {

	_, err := fmt.Fprintf(c.IncConn, "%s\r\n", text)
	if err != nil {
		return err
	}

	return nil
}

// Receive wraps the main io.Reader function that awaits text
// until an IMAP newline symbol and deletes the symbols after-
// wards again. It returns the resulting string or an error.
func (c *Connection) Receive() (string, error) {

	text, err := c.IncReader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\r\n"), nil
}
