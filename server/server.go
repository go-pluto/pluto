package server

import (
	"fmt"
	"log"
	"net"

	"github.com/numbleroot/pluto/imap"
)

// Structs

// Struct bundles information of one server instance.
type Server struct {
	IP     string
	Port   string
	Socket net.Listener
}

// Functions

// InitServer opens up a TCP socket on supplied
// IP address and port. It returns those information
// bundeled in above Server struct.
func InitServer(ip string, port string) *Server {

	var err error
	server := new(Server)

	// Place arguments in corresponding struct members.
	server.IP = ip
	server.Port = port

	// Start to listen on defined IP and port.
	server.Socket, err = net.Listen("tcp", fmt.Sprintf("%s:%s", server.IP, server.Port))
	if err != nil {
		log.Fatalf("[server.InitServer] Listening on port failed with: %s\n", err.Error())
	}

	log.Printf("[server.InitServer] Listening for incoming IMAP requests on %s.\n", server.Socket.Addr())

	return server
}

// HandleRequest acts as the jump start for any new
// incoming connection to pluto. It creates the needed
// control structure, sends out the initial server
// greeting and after that hands over control to the
// IMAP state machine.
func HandleRequest(conn net.Conn, greeting string) {

	log.Println("[DEBUG] New connection.")

	// Create a new connection struct for incoming request.
	c := imap.NewConnection(conn)

	log.Println("[DEBUG] Connection struct created.")

	// Send initial server greeting.
	err := c.Send("* OK IMAP4rev1 " + greeting)
	if err != nil {

		// If send returned a problem, the connection seems to be broken.
		// Log error and terminate this connection.
		log.Printf("[server.HandleRequest] Request terminated due to received Send error: %s\n", err.Error())

		return
	}

	log.Println("[DEBUG] Dispatching to not-authenticated state.")

	// Dispatch to not-authenticated state.
	c.Transition(imap.NOT_AUTHENTICATED)

	log.Println("[DEBUG] Connection dispatched.")
}

// RunServer loops over incoming requests and
// dispatches each one to a goroutine taking
// care of the commands supplied.
func (server *Server) RunServer(greeting string) {

	for {

		// Accept request or fail on error.
		conn, err := server.Socket.Accept()
		if err != nil {
			log.Fatalf("[server.RunServer] Accepting incoming request failed with: %s\n", err.Error())
		}

		// Dispatch to goroutine.
		go HandleRequest(conn, greeting)
	}
}
