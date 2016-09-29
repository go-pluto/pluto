package server

import (
	"fmt"
	"io"
	"log"
	"net"
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

func HandleRequest(c net.Conn) {

	log.Println("> Handling request.")

	// Copy request contents.
	io.Copy(c, c)
	c.Close()

	log.Println("> Request done.")
}

// RunServer loops over incoming requests and
// dispatches each one to a goroutine taking
// care of the commands supplied.
func (server *Server) RunServer() {

	for {

		// Accept request or fail on error.
		c, err := server.Socket.Accept()
		if err != nil {
			log.Fatalf("[server.RunServer] Accepting incoming request failed with: %s\n", err.Error())
		}

		// Dispatch to goroutine.
		go HandleRequest(c)
	}
}
