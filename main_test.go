package main

import (
	"bufio"
	"log"
	"net"
	"testing"
)

// Functions

func TestServe(t *testing.T) {

	server := InitServer()
	go Serve(server)

	conn, err := net.Dial("tcp", "127.0.0.1:1993")
	if err != nil {
		t.Errorf("[test serve] Connecting to IMAP server failed with: %s\n", err.Error())
	}

	log.Printf("[test serve]  Sending: LOGIN\n")
	conn.Write([]byte("LOGIN\n"))
	answer, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Errorf("[test serve] Could not receive answer: %s\n", err.Error())
	}
	log.Printf("[test serve] Received: %s\n", answer)

	conn.Close()
}
