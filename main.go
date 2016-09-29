package main

import (
	"flag"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/server"
)

// Functions

func main() {

	// Parse command-line flag that defines a config path.
	configFlag := flag.String("config", "config.toml", "Provide path to configuration file in YAML syntax.")
	flag.Parse()

	// Read configuration from file.
	Config := config.LoadConfig(*configFlag)

	// Load environment from .env file.
	// Env := config.LoadEnv()

	// Initialize a server instance.
	Server := server.InitServer(Config.IP, Config.Port)
	defer Server.Socket.Close()

	// Loop on incoming requests.
	Server.RunServer()
}
