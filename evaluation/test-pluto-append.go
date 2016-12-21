package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/emersion/go-imap/client"
	"github.com/numbleroot/pluto/utils"
)

// Functions

func main() {

	var err error
	var imapClient *client.Client

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

	// Create connection string to connect to.
	imapAddr := fmt.Sprintf("%s:%s", *host, *port)

	// Connect to remote pluto system.
	if *tls {
		imapClient, err = client.DialTLS(imapAddr, testEnv.TLSConfig)
	} else {
		imapClient, err = client.Dial(imapAddr)
	}

	if err != nil {
		log.Fatalf("[evaluation.TestPlutoAppend] Was unable to connect to remote IMAP server: %s\n", err.Error())
	}

	// Log in as first user.
	err = imapClient.Login("user0", "password0")
	if err != nil {
		log.Fatalf("[evaluation.TestPlutoAppend] Failed to login as 'user0': %s\n", err.Error())
	}

	// Log out on function exit.
	imapClient.Logout()

	// Select INBOX as mailbox.
	inbox, err := imapClient.Select("INBOX", false)
	if err != nil {
		log.Fatalf("[evaluation.TestPlutoAppend] Error during selecting 'INBOX': %s\n", err.Error())
	}

	log.Printf("inbox: %#v\n", inbox)

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
