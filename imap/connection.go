package imap

import (
	"bufio"
	"fmt"
	stdlog "log"
	"strings"

	"crypto/tls"
	"path/filepath"
)

// Constants

// Integer counter for IMAP states.
const (
	Any State = iota
	NotAuthenticated
	Authenticated
	Mailbox
	Logout
)

// Structs

// State represents the integer value associated with one
// of the implemented IMAP states a connection can be in.
type State int

// Structs

// Connection carries all information specific
// to one observed connection on its way through
// a pluto node that only authenticates and proxies
// IMAP connections.
type Connection struct {
	gRPCClient    NodeClient
	IncConn       *tls.Conn
	IncReader     *bufio.Reader
	IntlTLSConfig *tls.Config
	IsAuthorized  bool
	ClientID      string
	UserName      string
}

// IMAPConnection contains additional elements needed
// for performing the actual IMAP operations for an
// authenticated client.
type IMAPConnection struct {
	*Connection
	State           State
	UserCRDTPath    string
	UserMaildirPath string
	SelectedMailbox string
}

// Functions

// Send takes in an answer text from a node as a
// string and writes it to the connection to the client.
// In case an error occurs, this method returns it to
// the calling function.
func (c *Connection) Send(inc bool, text string) error {

	var err error

	// Check which attached connection should be used.
	conn := c.IncConn
	if inc != true {
		conn = c.OutConn
	}

	// Send message.
	_, err = fmt.Fprintf(conn, "%s\r\n", text)
	if err != nil {
		return err
	}

	return nil
}

// Receive wraps the main io.Reader function that awaits text
// until an IMAP newline symbol and deletes the symbols after-
// wards again. It returns the resulting string or an error.
func (c *Connection) Receive(inc bool) (string, error) {

	var err error

	// Check which attached connection should be used.
	conn := c.IncConn
	if inc != true {
		conn = c.OutConn
	}

	reader := c.IncReader
	if inc != true {
		reader = c.OutReader
	}

	// Initial value for received message in order
	// to skip past the mandatory ping message.
	text := "> ping <\r\n"

	for text == "> ping <\r\n" {

		text, err = reader.ReadString('\n')
		if err != nil {

			if err.Error() == "EOF" {
				stdlog.Printf("[imap.Receive] Node at '%s' disconnected.", conn.RemoteAddr().String())
			}

			break
		}
	}

	// If an error happened, return it.
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\r\n"), nil
}

// Terminate tears down the state of a connection.
// This includes closing contained connection items.
// It returns nil or eventual errors.
func (c *Connection) Terminate() error {

	if c.IncConn != nil {

		// Possibly close incoming connection.
		err := c.IncConn.Close()
		if err != nil {
			return err
		}
	}

	if c.OutConn != nil {

		// Possibly close outgoing connection.
		err := c.OutConn.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Error makes use of Terminate but provides an additional
// error message before terminating.
func (c *Connection) Error(msg string, err error) {

	// Log error.
	stdlog.Printf("%s: %v. Terminating connection to %s.", msg, err, c.IncConn.RemoteAddr().String())

	// Terminate connection.
	err = c.Terminate()
	if err != nil {
		stdlog.Fatal(err)
	}
}

// UpdateClientContext expects the initial client information
// string sent when the proxying node opened a connection to
// a worker node. It updates the existing connection with the
// contained information.
func (c *IMAPConnection) UpdateClientContext(clientIDRaw string, CRDTLayerRoot string, MaildirRoot string) (string, error) {

	// Split received clientID string at white spaces
	// and check for correct amount of found fields.
	fields := strings.Fields(clientIDRaw)
	if (len(fields) != 4) && (len(fields) != 5) {
		return "", fmt.Errorf("received an invalid clientID information")
	}

	if len(fields) == 4 {

		// Check if structure for worker node is correct.
		if fields[0] != ">" || fields[1] != "id:" || fields[3] != "<" {
			return "", fmt.Errorf("received an invalid clientID information")
		}

	} else if len(fields) == 5 {

		// Check if structure for storage node is correct.
		if fields[0] != ">" || fields[1] != "id:" || fields[4] != "<" {
			return "", fmt.Errorf("received an invalid clientID information")
		}

	}

	// Parse parts including user name from clientID.
	clientInfo := strings.SplitN(fields[2], ":", 3)

	// Update existing IMAP connection.
	c.ClientID = fields[2]
	c.UserName = clientInfo[2]
	c.UserCRDTPath = filepath.Join(CRDTLayerRoot, clientInfo[2])
	c.UserMaildirPath = filepath.Join(MaildirRoot, clientInfo[2])

	return fields[3], nil
}
