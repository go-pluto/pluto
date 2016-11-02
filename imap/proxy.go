package imap

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/numbleroot/pluto/conn"
)

// Functions

func (node *Node) Proxy(c *conn.Connection) error {

	// We need proper auxiliary variables for later access.
	connWorker := node.Connections[*c.Worker]
	readerWorker := bufio.NewReader(connWorker)

	// Set loop end condition initially to this state.
	nextState := conn.AUTHENTICATED

	for nextState != conn.LOGOUT {

		// Receive incoming client command.
		rawReq, err := c.Receive()
		if err != nil {
			return err
		}

		// Parse received raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				return err
			}

			// Go back to beginning of for loop.
			continue
		}

		// If client closes connection, set end
		// condition for loop.
		if req.Command == "LOGOUT" {
			nextState = conn.LOGOUT
		}

		// Send received client command to worker node.
		if _, err := fmt.Fprintf(connWorker, "%s\n", rawReq); err != nil {
			return err
		}

		// Reserve space for answer buffer.
		bufResp := make([]string, 0, 2)

		// Receive incoming worker response.
		curResp, err := readerWorker.ReadString('\n')
		if err != nil {
			return err
		}
		curResp = strings.TrimRight(curResp, "\n")

		// As long as the responsible worker has not
		// indicated the end of the current operation,
		// continue to buffer answers.
		for curResp != "> done <" {

			// Append it to answer buffer.
			bufResp = append(bufResp, curResp)

			// Receive incoming worker response.
			curResp, err = readerWorker.ReadString('\n')
			if err != nil {
				return err
			}
			curResp = strings.TrimRight(curResp, "\n")
		}

		for i := range bufResp {

			// Send all buffered worker answers to client.
			err = c.Send(bufResp[i])
			if err != nil {
				return err
			}
		}
	}

	return nil
}
