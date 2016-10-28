// +build ignore

package main

import (
	"log"

	"crypto/tls"

	"github.com/numbleroot/pluto/crypto"
)

func main() {

	log.Println("First")

	config, err := crypto.NewInternalTLSConfig("private/internal-worker-1-cert.pem", "private/internal-worker-1-key.pem", "private/root-cert.pem")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Second")

	c, err := tls.Dial("tcp", "127.0.0.1:21000", config)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Third")

	c.Close()
}
