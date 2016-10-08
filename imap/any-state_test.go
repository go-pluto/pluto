package imap_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/server"
)

// Structs

var capabilityTests = []struct {
	in  string
	out string
}{
	{"a001 CAPABILITY", "* CAPABILITY IMAP4rev1 LOGINDISABLED AUTH=PLAIN\na001 OK CAPABILITY completed"},
	{"1337 capability", "* CAPABILITY IMAP4rev1 LOGINDISABLED AUTH=PLAIN\n1337 OK CAPABILITY completed"},
	{"tag CAPABILITY   ", "tag BAD Command CAPABILITY was sent with extra parameters"},
	{"CAPABILITY", "* BAD Received invalid IMAP command"},
}

var loginTests = []struct {
	in  string
	out string
}{
	{"xyz LOGIN smith sesame", "xyz NO Command LOGIN is disabled"},
	{"zyx login smith sesame", "zyx NO Command LOGIN is disabled"},
	{"a1b2c3   LOGIN    smith sesame", "a1b2c3 BAD Received invalid IMAP command"},
	{"LOGIN ernie bert", "* BAD Received invalid IMAP command"},
	{"12345 LOL ernie bert", "12345 BAD Received invalid IMAP command"},
}

var logoutTests = []struct {
	in  string
	out string
}{
	{"iiqqee LOGOUT", "* BYE Terminating connection\niiqqee OK LOGOUT completed"},
	{"lol logout", "* BYE Terminating connection\nlol OK LOGOUT completed"},
	{"5   LOGOUT    ", "5 BAD Received invalid IMAP command"},
	{"LOGOUT some more parameters", "* BAD Received invalid IMAP command"},
	{"b01 LOGOUT some more parameters", "b01 BAD Command LOGOUT was sent with extra parameters"},
}

// Variables

var Config *config.Config
var Server *server.Server
var TLSConfig *tls.Config

// Functions

func TestMain(m *testing.M) {

	// Read configuration from file.
	Config = config.LoadConfig("../config.toml")

	// Initialize a server instance.
	Server = server.InitServer(Config.IP, Config.Port, Config.TLS.CertLoc, Config.TLS.KeyLoc)

	// Read in server certificate and create x509 cert pool.
	TLSConfig = &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		InsecureSkipVerify:       false,
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	// Create new certificate pool to hold server certificate.
	rootCerts := x509.NewCertPool()

	// Read server certificate specified in pluto's main
	// config file into memory.
	rootCert, err := ioutil.ReadFile(Config.TLS.CertLoc)
	if err != nil {
		log.Fatalf("[imap.TestMain] Reading server certificate into memory failed with: %s\n", err.Error())
	}

	// Append certificate in PEM form to pool.
	ok := rootCerts.AppendCertsFromPEM(rootCert)
	if !ok {
		log.Fatalf("[imap.TestMain] Failed to append certificate to pool: %s\n", err.Error())
	}

	// Now make created pool the root pool
	// of above global TLS config.
	TLSConfig.RootCAs = rootCerts

	// Start test server in background.
	go Server.RunServer(Config.IMAP.Greeting)

	// Start main tests.
	os.Exit(m.Run())
}

// TestCapability executes a black-box table test on the
// implemented Capability() function.
func TestCapability(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (Config.IP + ":" + Config.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestCapability] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestCapability] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for i, tt := range capabilityTests {

		var answer string

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestCapability] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive options listed in CAPABILITY command.
		capAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestCapability] Error during receiving table test CAPABILITY: %s\n", err.Error())
		}

		if i < 2 {

			// Receive command termination message from server.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestCapability] Error during receiving table test CAPABILITY: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", capAnswer, okAnswer)
		} else {
			answer = capAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestCapability] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestLogin executes a black-box table test on the
// implemented Login() function.
func TestLogin(t *testing.T) {

	// Connect to IMAP server.
	conn, err := tls.Dial("tcp", (Config.IP + ":" + Config.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[imap.TestLogin] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_, err = c.Receive()
	if err != nil {
		t.Errorf("[imap.TestLogin] Error during receiving initial server greeting: %s\n", err.Error())
	}

	for _, tt := range loginTests {

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogin] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive LOGINDISABLED answer.
		answer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogin] Error during receiving table test LOGIN: %s\n", err.Error())
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogin] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}

	// At the end of each test, terminate connection.
	c.Terminate()
}

// TestLogout executes a black-box table test on the
// implemented Logout() function.
func TestLogout(t *testing.T) {

	for i, tt := range logoutTests {

		var answer string

		// Connect to IMAP server.
		conn, err := tls.Dial("tcp", (Config.IP + ":" + Config.Port), TLSConfig)
		if err != nil {
			t.Fatalf("[imap.TestLogout] Error during connection attempt to IMAP server: %s\n", err.Error())
		}

		// Create new connection struct.
		c := imap.NewConnection(conn)

		// Consume mandatory IMAP greeting.
		_, err = c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogout] Error during receiving initial server greeting: %s\n", err.Error())
		}

		// Table test: send 'in' part of each line.
		err = c.Send(tt.in)
		if err != nil {
			t.Fatalf("[imap.TestLogout] Sending message to server failed with: %s\n", err.Error())
		}

		// Receive logout reponse.
		logoutAnswer, err := c.Receive()
		if err != nil {
			t.Errorf("[imap.TestLogout] Error during receiving table test LOGOUT: %s\n", err.Error())
		}

		if i < 2 {

			// Receive command termination message from server.
			okAnswer, err := c.Receive()
			if err != nil {
				t.Errorf("[imap.TestLogout] Error during receiving table test LOGOUT: %s\n", err.Error())
			}

			answer = fmt.Sprintf("%s\n%s", logoutAnswer, okAnswer)
		} else {
			answer = logoutAnswer
		}

		if answer != tt.out {
			t.Fatalf("[imap.TestLogout] Expected '%s' but received '%s'\n", tt.out, answer)
		}

		// At the end of each test, terminate connection.
		err = c.Terminate()
		if err != nil {
			log.Fatal(err)
		}
	}
}
