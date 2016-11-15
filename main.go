package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
)

// Functions

func main() {

	var err error

	// Set CPUs usable by pluto to all available.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Parse command-line flag that defines a config path.
	configFlag := flag.String("config", "config.toml", "Provide path to configuration file in TOML syntax.")
	distributorFlag := flag.Bool("distributor", false, "Append this flag to indicate that this process should take the role of the distributor.")
	workerFlag := flag.String("worker", "", "If this process is intended to run as one of the IMAP worker nodes, specify which of the ones defined in your config file this should be.")
	storageFlag := flag.Bool("storage", false, "Append this flag to indicate that this process should take the role of the storage node.")
	flag.Parse()

	// Read configuration from file.
	conf, err := config.LoadConfig(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	// Load environment from .env file.
	err = config.LoadEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Initialize and run a node of the pluto
	// system based on passed command line flag.
	if *distributorFlag {

		// Initialize distributor.
		distr, err := imap.InitDistributor(conf)
		if err != nil {
			log.Fatal(err)
		}
		defer distr.Socket.Close()

		// Loop on incoming requests.
		err = distr.Run()
		if err != nil {
			log.Fatal(err)
		}
	} else if *workerFlag != "" {

		// Initialize worker.
		worker, err := imap.InitWorker(conf, *workerFlag)
		if err != nil {
			log.Fatal(err)
		}
		defer worker.MailSocket.Close()

		// Loop on incoming requests.
		err = worker.Run()
		if err != nil {
			log.Fatal(err)
		}
	} else if *storageFlag {

		// Initialize storage.
		storage, err := imap.InitStorage(conf)
		if err != nil {
			log.Fatal(err)
		}
		defer storage.Socket.Close()

		// Loop on incoming requests.
		err = storage.Run()
		if err != nil {
			log.Fatal(err)
		}
	}
}
