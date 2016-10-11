package server

import (
	"fmt"
	"log"
	"net"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
)

// Structs

// Server struct bundles information of one server instance.
type Server struct {
	IP     string
	Port   string
	Socket net.Listener
}

// Functions

// InitServer listens for TLS connections on a TCP socket
// opened up on supplied IP address and port. It returns
// those information bundeled in above Server struct.
func InitServer(config *config.Config) *Server {

	var err error
	server := new(Server)

	// Place arguments in corresponding struct members.
	server.IP = config.IP
	server.Port = config.Port

	// TLS config is taken from the excellent blog post
	// "Achieving a Perfect SSL Labs Score with Go":
	// https://blog.bracelab.com/achieving-perfect-ssl-labs-score-with-go
	tlsConfig := &tls.Config{
		Certificates:             make([]tls.Certificate, 1),
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	// Put in supplied TLS cert and key.
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(config.TLS.CertLoc, config.TLS.KeyLoc)
	if err != nil {
		log.Fatalf("[server.InitServer] Failed to load TLS cert and key: %s\n", err.Error())
	}

	// Build Common Name (CN) and Subject Alternate
	// Name (SAN) from tlsConfig.Certificates.
	tlsConfig.BuildNameToCertificate()

	// Start to listen on defined IP and port.
	server.Socket, err = tls.Listen("tcp", fmt.Sprintf("%s:%s", server.IP, server.Port), tlsConfig)
	if err != nil {
		log.Fatalf("[server.InitServer] Listening for TLS connections on port failed with: %s\n", err.Error())
	}

	log.Printf("[server.InitServer] Listening for incoming IMAP requests on %s.\n", server.Socket.Addr())

	return server
}

// HandleRequest acts as the jump start for any new
// incoming connection to pluto. It creates the needed
// control structure, sends out the initial server
// greeting and after that hands over control to the
// IMAP state machine.
func (server *Server) HandleRequest(conn net.Conn, greeting string) {

	// Create a new connection struct for incoming request.
	c := imap.NewConnection(conn)

	// Send initial server greeting.
	err := c.Send("* OK IMAP4rev1 " + greeting)
	if err != nil {

		// If send returned a problem, the connection seems to be broken.
		// Log error and terminate this connection.
		log.Printf("[server.HandleRequest] Request terminated due to received Send error: %s\n", err.Error())

		return
	}

	// Dispatch to not-authenticated state.
	c.Transition(imap.NOT_AUTHENTICATED)
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
		go server.HandleRequest(conn, greeting)
	}
}
