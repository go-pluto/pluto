package node

import (
	"log"
	"os"
	"testing"
	"time"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
)

// Variables

var Config *config.Config
var TLSConfig *tls.Config

// Functions

// TestMain initializes structures needed later on.
func TestMain(m *testing.M) {

	// Provide a correct config.
	Config = &config.Config{
		Distributor: config.Distributor{
			IP:   "127.0.0.1",
			Port: "19933",
			TLS: config.TLS{
				CertLoc: "../private/cert.pem",
				KeyLoc:  "../private/key.pem",
			},
			IMAP: config.IMAP{
				Greeting: "Pluto ready.",
			},
			Auth: config.Auth{
				Adaptor:  "postgres",
				IP:       "127.0.0.1",
				Port:     "5432",
				Database: "pluto",
				User:     "pluto",
			},
		},
		Workers: map[string]config.Worker{
			"worker-01": {
				IP:        "127.0.0.1",
				Port:      "20001",
				UserStart: 1,
				UserEnd:   10,
			},
		},
		Storage: config.Storage{
			IP:   "127.0.0.1",
			Port: "21000",
		},
	}

	// Read in distributor certificate and create x509 cert pool.
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

	// Create new certificate pool to hold distributor certificate.
	rootCerts := x509.NewCertPool()

	// Read distributor certificate specified in above config into memory.
	rootCert, err := ioutil.ReadFile(Config.Distributor.TLS.CertLoc)
	if err != nil {
		log.Fatalf("[node.TestMain] Reading distributor certificate into memory failed with: %s\n", err.Error())
	}

	// Append certificate in PEM form to pool.
	ok := rootCerts.AppendCertsFromPEM(rootCert)
	if !ok {
		log.Fatalf("[node.TestMain] Failed to append certificate to pool: %s\n", err.Error())
	}

	// Now make created pool the root pool
	// of above global TLS config.
	TLSConfig.RootCAs = rootCerts

	// Start main tests.
	os.Exit(m.Run())
}

// TestInitNode executes a white-box unit test on the
// implemented InitNode() function.
func TestInitNode(t *testing.T) {

	var Node *Node
	var err error

	// All types indicated.
	_, err = InitNode(Config, true, "worker-01", true)
	if err.Error() != "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, true, "abc", false)
	if err.Error() != "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, true, "", true)
	if err.Error() != "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, false, "worker-01", true)
	if err.Error() != "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// No type indicated.
	_, err = InitNode(Config, false, "", false)
	if err.Error() != "[node.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Non-existent worker ID.
	_, err = InitNode(Config, false, "abc", false)
	if err.Error() != "[node.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-01'.\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-01'.\n", err.Error())
	}

	// Set certificate path in config to non-existent one.
	Config.Distributor.TLS.CertLoc = "../private/not-existing.cert"

	// Config error: non-existent certificate.
	_, err = InitNode(Config, true, "", false)
	if err.Error() != "[node.InitNode] Failed to load distributor TLS cert and key: open ../private/not-existing.cert: no such file or directory\n" {
		t.Fatalf("[node.TestInitNode] Expected '%s' but received '%s'\n", "[node.InitNode] Failed to load distributor TLS cert and key: open ../private/not-existing.cert: no such file or directory\n", err.Error())
	}

	// Set wrong certificate path back to correct one.
	Config.Distributor.TLS.CertLoc = "../private/cert.pem"

	// Correct distributor initialization.
	Node, err = InitNode(Config, true, "", false)
	Node.Socket.Close()

	// Correct worker initialization.
	Node, err = InitNode(Config, false, "worker-01", false)
	Node.Socket.Close()

	// Correct storage initialization.
	Node, err = InitNode(Config, false, "", true)
	Node.Socket.Close()
}

// TestRunNode executes a white-box unit test on the
// implemented RunNode() and HandleRequest() functions.
func TestRunNode(t *testing.T) {

	// Initialize a correct node.
	Node, err := InitNode(Config, true, "", false)
	if err != nil {
		t.Fatalf("[node.TestRunNode] Error during initializing node with correct config: %s\n", err.Error())
	}
	defer Node.Socket.Close()

	// Call RunNode in background.
	go func() {

		err := Node.RunNode("Very special custom greeting.")
		if err.Error() != "[node.RunNode] Accepting incoming request failed with: accept tcp 127.0.0.1:19933: use of closed network connection\n" {
			t.Fatalf("[node.RunNode] Expected '%s' but received '%s'\n", "[node.RunNode] Accepting incoming request failed with: accept tcp 127.0.0.1:19933: use of closed network connection\n", err.Error())
		}
	}()

	// Wait shortly for backend before sending request.
	time.Sleep(time.Millisecond * 100)

	// Connect to IMAP distributor.
	conn, err := tls.Dial("tcp", (Config.Distributor.IP + ":" + Config.Distributor.Port), TLSConfig)
	if err != nil {
		t.Fatalf("[node.TestRunNode] Error during connection attempt to IMAP distributor: %s\n", err.Error())
	}
	defer conn.Close()

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Expect to receive IMAP greeting.
	imapGreeting, err := c.Receive()
	if err != nil {
		t.Errorf("[imap.TestRunNode] Error during receiving initial distributor greeting: %s\n", err.Error())
	}

	// Check if greeting is the one we expect.
	if imapGreeting != "* OK IMAP4rev1 Very special custom greeting." {
		t.Fatalf("[node.TestRunNode] Expected '%s' but received '%s'\n", "* OK IMAP4rev1 Very special custom greeting.", imapGreeting)
	}
}
