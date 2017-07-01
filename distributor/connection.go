package distributor

import (
	"bufio"
	"fmt"
	"strings"

	"crypto/tls"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/imap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Structs

// Connection carries all information specific
// to one observed connection on its way through
// a pluto node that only authenticates and proxies
// IMAP connections.
type Connection struct {
	gRPCConn      *grpc.ClientConn
	gRPCClient    imap.NodeClient
	IncConn       *tls.Conn
	IncReader     *bufio.Reader
	IsAuthorized  bool
	ClientID      string
	ClientAddr    string
	UserName      string
	PrimaryNode   string
	PrimaryAddr   string
	SecondaryNode string
	SecondaryAddr string
	ActualNode    string
	ActualAddr    string
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

// Connect to primary node or fail over to secondary node
// in case of an error. If failover fails as well, go back
// to primary node.
func (c *Connection) Connect(opts []grpc.DialOption, logger log.Logger, sendPrepare bool) {

	// TODO: Cancel this if client disconnects while execution.

	c.ActualNode = c.PrimaryNode
	c.ActualAddr = c.PrimaryAddr

	// Close lost or broken connection before attempting to
	// connect to secondary node, if existent.
	if c.gRPCConn != nil {

		err := c.gRPCConn.Close()
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to close lost or broken client connection to %s", c.PrimaryNode),
				"err", err,
			)
		}
	}

	// Dial primary node.
	conn, err := grpc.Dial(c.PrimaryAddr, opts...)
	for err != nil {

		// Failed. Switch actual node representation.
		level.Debug(logger).Log(
			"msg", fmt.Sprintf("failed to dial to %s (%s), failing over to %s...", c.PrimaryNode, c.PrimaryAddr, c.SecondaryNode),
			"err", err,
		)
		c.ActualNode = c.SecondaryNode
		c.ActualAddr = c.SecondaryAddr

		// Fail over to secondary node.
		conn, err = grpc.Dial(c.SecondaryAddr, opts...)
		if err != nil {

			// Failed. Switch actual node representation.
			level.Debug(logger).Log(
				"msg", fmt.Sprintf("failed to dial to %s (%s), trying %s again...", c.SecondaryNode, c.SecondaryAddr, c.PrimaryNode),
				"err", err,
			)
			c.ActualNode = c.PrimaryNode
			c.ActualAddr = c.PrimaryAddr

			// Dial primary node again.
			conn, err = grpc.Dial(c.PrimaryAddr, opts...)
		}
	}

	level.Debug(logger).Log("msg", fmt.Sprintf("dialled to %s (%s)", c.ActualNode, c.ActualAddr))

	// Bootstrap gRPC client from established connection.
	c.gRPCConn = conn
	c.gRPCClient = imap.NewNodeClient(conn)

	// If specified, send connected node context of to-come client connection.
	if sendPrepare {

		_, _ = c.gRPCClient.Prepare(context.Background(), &imap.Context{
			ClientID:   c.ClientID,
			UserName:   c.UserName,
			RespWorker: c.PrimaryNode,
		})
	}
}
