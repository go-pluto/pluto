package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/emersion/go-imap/client"
	"github.com/numbleroot/pluto/utils"
)

func main() {

	var err error
	var c *client.Client

	// Create environment we need in order to test
	// against public part of pluto.
	_, tlsConfig, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	mailHost := flag.String("host", "", "name or ip address (required)")
	mailPort := flag.Int("port", 0, "port (required)")
	mailUser := flag.String("user", "", "username (required)")
	mailPassword := flag.String("pass", "", "password (required)")
	mailOutput := flag.String("output", "", "output file (required)")

	// Following two are optional.
	mailSSL := flag.Bool("ssl", false, "boolean")
	mailMessages := flag.Int("messages", 100, "number of messages")

	flag.Parse()

	if len(*mailHost) == 0 || len(*mailUser) == 0 || len(*mailOutput) == 0 || len(*mailPassword) == 0 || *mailPort == 0 {
		log.Fatal("Not enough arguments, see -h. Exiting.")
	}

	log.Println("Connecting to server...")

	if *mailSSL {
		c, err = client.DialTLS(fmt.Sprintf("%s:%d", *mailHost, *mailPort), tlsConfig)
	} else {
		c, err = client.Dial(*mailHost + ":" + strconv.Itoa(*mailPort))
	}

	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected")

	if err := c.Login(*mailUser, *mailPassword); err != nil {
		log.Fatal(err)
	}

	log.Println("Logged in")

	msg := "From: John Doe <jdoe@machine.example>\r\n"
	msg = msg + "To: Mary Smith <mary@example.net>\r\n"
	msg = msg + "Subject: Saying Hello\r\n"
	msg = msg + "Date: Fri, 21 Nov 1814 09:55:06 -0600\r\n"
	msg = msg + "Message-ID: <1234@local.machine.example>\r\n\r\n"
	msg = msg + "yolo\r\n"

	date := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	literal := bytes.NewBufferString(msg)

	log.Println("Appending Items")

	f, err := os.OpenFile(*mailOutput, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	for i := 0; i < *mailMessages; i++ {
		t1 := time.Now()
		if err := c.Append("INBOX", []string{"\\Draft"}, date, literal); err != nil {
			log.Fatal(err)
		} else {
			t2 := time.Now()
			diff := t2.Sub(t1)
			log.Println(diff)
			if _, err = f.WriteString(strconv.Itoa(i) + ", " + diff.String() + "\r\n"); err != nil {
				log.Fatal(err)
			}
		}
	}

	f.Close()

	log.Println("Logout")
	defer c.Logout()

	log.Println("Done!")
}
