package utils

import (
	"fmt"
	"log"
	"time"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
	"github.com/numbleroot/pluto/imap"
)

// Structs

// TestEnv carries everything needed for a full
// grown test of pluto with multiple nodes.
type TestEnv struct {
	Config      *config.Config
	TLSConfig   *tls.Config
	DownDistr   chan struct{}
	DownWorker  chan struct{}
	DownStorage chan struct{}
	DoneDistr   chan struct{}
	DoneWorker  chan struct{}
	DoneStorage chan struct{}
}

// CreateTestEnv initializes the needed environment
// for performing various tests against a potentially
// complete pluto setup.
func CreateTestEnv() (*TestEnv, error) {

	// Read configuration from file.
	config, err := config.LoadConfig("../test-config.toml")
	if err != nil {
		return nil, err
	}

	// Create public TLS config (distributor).
	tlsConfig, err := crypto.NewPublicTLSConfig(config.Distributor.PublicTLS.CertLoc, config.Distributor.PublicTLS.KeyLoc)
	if err != nil {
		return nil, err
	}

	// For tests, we currently need to build a custom
	// x509 cert pool to accept the self-signed public
	// distributor certificate.
	tlsConfig.RootCAs = x509.NewCertPool()

	// Read distributor's public certificate in PEM format
	// specified in pluto's main config file into memory.
	rootCert, err := ioutil.ReadFile(config.Distributor.PublicTLS.CertLoc)
	if err != nil {
		return nil, fmt.Errorf("[utils.CreateTestEnv] Reading distributor's public certificate into memory failed with: %s\n", err.Error())
	}

	// Append certificate to test client's root CA pool.
	if ok := tlsConfig.RootCAs.AppendCertsFromPEM(rootCert); !ok {
		return nil, fmt.Errorf("[utils.CreateTestEnv] Failed to append certificate to pool: %s\n", err.Error())
	}

	// Return properly initilized and complete struct
	// representing a test environment.
	return &TestEnv{
		Config:      config,
		TLSConfig:   tlsConfig,
		DownDistr:   make(chan struct{}),
		DownWorker:  make(chan struct{}),
		DownStorage: make(chan struct{}),
		DoneDistr:   make(chan struct{}),
		DoneWorker:  make(chan struct{}),
		DoneStorage: make(chan struct{}),
	}, nil
}

// RunAllNodes runs all needed nodes for a proper multi-node
// test setup in background. It also handles shutdown of these
// nodes when appropriate signals are received.
func RunAllNodes(testEnv *TestEnv, workerName string) {

	go func() {

		// Initialize storage node.
		storage, err := imap.InitStorage(testEnv.Config)
		if err != nil {
			log.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownStorage

			log.Printf("[utils.RunAllNodes] Closing storage socket.\n")

			// Shut down storage node.
			storage.MailSocket.Close()
			storage.SyncSocket.Close()

			// Signal back successful shutdown.
			testEnv.DoneStorage <- struct{}{}
		}()

		// Run the storage node.
		_ = storage.Run()
	}()

	// Wait shortly for storage node to have started.
	time.Sleep(100 * time.Millisecond)

	go func() {

		// Initialize workerName worker node.
		worker, err := imap.InitWorker(testEnv.Config, workerName)
		if err != nil {
			log.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownWorker

			log.Printf("[utils.RunAllNodes] Closing %s socket.\n", workerName)

			// Shut down worker node.
			worker.MailSocket.Close()
			worker.SyncSocket.Close()

			// Signal back successful shutdown.
			testEnv.DoneWorker <- struct{}{}
		}()

		// Run the worker node.
		_ = worker.Run()
	}()

	// Wait shortly for worker node to have started.
	time.Sleep(100 * time.Millisecond)

	go func() {

		// Initialize distributor node.
		distr, err := imap.InitDistributor(testEnv.Config)
		if err != nil {
			log.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownDistr

			log.Printf("[utils.RunAllNodes] Closing distributor socket.\n")

			// Shut down distributor node.
			distr.Socket.Close()

			// Signal back successful shutdown.
			testEnv.DoneDistr <- struct{}{}
		}()

		// Run the distributor node.
		_ = distr.Run()
	}()

	// Wait shortly for worker node to have started.
	time.Sleep(500 * time.Millisecond)
}

// TearDownNodes takes up the role of shutting down the
// running nodes by sending shutdown signals and expecting
// success answers.
func TearDownNodes(testEnv *TestEnv) {

	// Signal to all nodes running background that they
	// are supposed to shut down now.
	testEnv.DownDistr <- struct{}{}
	testEnv.DownWorker <- struct{}{}
	testEnv.DownStorage <- struct{}{}

	// Wait for them to signal success back.
	<-testEnv.DoneDistr
	<-testEnv.DoneWorker
	<-testEnv.DoneStorage
}
