package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
)

func main() {

	mailHost := flag.String("host", "", "name or ip adress (required)")
	mailPort := flag.Int("port", 0, "port (required)")
	mailUser := flag.String("user", "", "username (required)")
	mailPassword := flag.String("pass", "", "password (required)")
	mailOutput := flag.String("output", "", "output file (required)")
	mailSSL := flag.Bool("ssl", false, "boolean")                   //optional
	mailMessages := flag.Int("messages", 100, "number of messages") //optional

	flag.Parse()

	if len(*mailHost) == 0 || len(*mailUser) == 0 || len(*mailOutput) == 0 || len(*mailPassword) == 0 || *mailPort == 0 {
		log.Fatal("not enough arguments, try -h")
	}

	log.Println("Connecting to server...")

	var c *client.Client = nil
	var err error = nil

	if *mailSSL {
		c, err = client.DialTLS(*mailHost+":"+strconv.Itoa(*mailPort), nil)
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
	literal := imap.NewLiteral([]byte(msg))

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
