package main

import (
	"flag"
	"io"
	"log"
	"net"

	"github.com/numbleroot/pluto/config"
)

// Functions

func InitServer() net.Listener {

	server, err := net.Listen("tcp", ":1993")
	if err != nil {
		log.Fatalf("[main] Listening on port failed with: %s\n", err.Error())
	}

	return server
}

func Serve(server net.Listener) {

	// Loop over incoming requests and dispatch
	// each one to a goroutine taking care of
	// the commands supplied.
	for {

		conn, err := server.Accept()
		if err != nil {
			log.Fatalf("[main] Accepting incoming request failed with: %s\n", err.Error())
		}

		io.Copy(conn, conn)

		conn.Close()
	}
}

func main() {

	// Parse command-line flags.
	configFlag := flag.String("config", "config.toml", "Provide path to configuration file in YAML syntax.")
	flag.Parse()

	// Read configuration from file.
	Config := config.LoadConfig(*configFlag)

	// Load environment from .env file.
	Env := config.LoadEnv()

	log.Println(Config.IP, Env.Secret)

	// Initialize imap server.
	server := InitServer()
	defer server.Close()

	log.Println(server.Addr())

	Serve(server)
}
