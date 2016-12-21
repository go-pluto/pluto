package main

import (
	"flag"
	"log"

	"github.com/emersion/go-imap/client"
	"github.com/numbleroot/pluto/utils"
)

// Functions

func main() {

	// Parse command-line flags:
	// remote IP, remote port, remote TLS support.
	host := flag.String("host", "127.0.0.1", "Declare to which domain or IP to connect to for sending IMAP traffic.")
	port := flag.String("port", "993", "Declare to which port to connect to for sending IMAP traffic.")
	tls := flag.Bool("tls", true, "Set to true if remote host allows for TLS encrypted connections.")

	flag.Parse()

	// Create test environment in order to obtain
	// the TLS config to connect publicly to pluto.
	testEnv, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatalf("[evaluation.TestPlutoAppend] Could not create test environment: %s\n", err.Error())
	}

	// Connect to remote pluto system.

	// For each mail to append:
	// * take current time stamp A
	// * prepare log line
	// * send mail to remote system
	// * wait for response
	// * log reponse time stamp B
	// * calculate rtt = B - A
	// * finish log line and append to test log

	// Calculate statistics and print them.

	// Close log file and exit.
}
