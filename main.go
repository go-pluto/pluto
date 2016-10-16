package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/node"
)

// Functions

func main() {

	// Set CPUs usable by pluto to all available.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Parse command-line flag that defines a config path.
	configFlag := flag.String("config", "config.toml", "Provide path to configuration file in TOML syntax.")
	distributorFlag := flag.Bool("distributor", false, "Append this flag to indicate that this process should take the role of the distributor.")
	workerFlag := flag.String("worker", "", "If this process is intended to run as one of the IMAP worker nodes, specify which of the ones defined in your config file this should be.")
	storageFlag := flag.Bool("storage", false, "Append this flag to indicate that this process should take the role of the storage node.")
	flag.Parse()

	// Read configuration from file.
	Config, err := config.LoadConfig(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	// Load environment from .env file.
	// Env, err := config.LoadEnv()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Initialize a server instance.
	Node := node.InitNode(Config, *distributorFlag, *workerFlag, *storageFlag)
	defer Node.Socket.Close()

	// Loop on incoming requests.
	Node.RunNode(Config.Distributor.IMAP.Greeting)
}
