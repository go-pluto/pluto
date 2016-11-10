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

// Functions

// CreateTestEnv initializes the needed environment
// for performing various tests against parts of
// the pluto system.
func CreateTestEnv() (*config.Config, *tls.Config, error) {

	var err error

	// Read configuration from file.
	config, err := config.LoadConfig("test-config.toml")
	if err != nil {
		return nil, nil, err
	}

	// Create public TLS config (distributor).
	tlsConfig, err := crypto.NewPublicTLSConfig(config.Distributor.PublicTLS.CertLoc, config.Distributor.PublicTLS.KeyLoc)
	if err != nil {
		return nil, nil, err
	}

	// For tests, we currently need to build a custom
	// x509 cert pool to accept the self-signed public
	// distributor certificate.
	tlsConfig.RootCAs = x509.NewCertPool()

	// Read distributor's public certificate in PEM format
	// specified in pluto's main config file into memory.
	rootCert, err := ioutil.ReadFile(config.Distributor.PublicTLS.CertLoc)
	if err != nil {
		return nil, nil, fmt.Errorf("[utils.CreateTestEnv] Reading distributor's public certificate into memory failed with: %s\n", err.Error())
	}

	// Append certificate to test client's root CA pool.
	if ok := tlsConfig.RootCAs.AppendCertsFromPEM(rootCert); !ok {
		return nil, nil, fmt.Errorf("[utils.CreateTestEnv] Failed to append certificate to pool: %s\n", err.Error())
	}

	return config, tlsConfig, nil
}

// RunStorageWithTimeout is supposed to be called in a goroutine
// and initializes and runs a storage node and shuts it down
// after waitMilliseconds of milliseconds.
func RunStorageWithTimeout(conf *config.Config, waitMilliseconds int) {

	// Initialize storage node.
	storage, err := imap.InitStorage(conf)
	if err != nil {
		log.Fatal(err)
	}

	// Close the socket after 500ms.
	time.AfterFunc((time.Duration(waitMilliseconds) * time.Millisecond), func() {
		storage.Socket.Close()
	})

	// Run the storage node.
	_ = storage.Run()
}

// RunWorkerWithTimeout is supposed to be called in a goroutine
// and initializes and runs a worker node and shuts it down
// after waitMilliseconds of milliseconds.
func RunWorkerWithTimeout(conf *config.Config, workerName string, waitMilliseconds int) {

	// Initialize workerName worker node.
	worker, err := imap.InitWorker(conf, workerName)
	if err != nil {
		log.Fatal(err)
	}

	// Close the socket after 500ms.
	time.AfterFunc((time.Duration(waitMilliseconds) * time.Millisecond), func() {
		worker.Socket.Close()
	})

	// Run the worker node.
	_ = worker.Run()
}

// RunDistributorWithTimeout is supposed to be called in a goroutine
// and initializes and runs a distributor node and shuts it down
// after waitMilliseconds of milli seconds.
func RunDistributorWithTimeout(conf *config.Config, waitMilliseconds int) {

	// Initialize distributor node.
	distr, err := imap.InitDistributor(conf)
	if err != nil {
		log.Fatal(err)
	}

	// Close the socket after 500ms.
	time.AfterFunc((time.Duration(waitMilliseconds) * time.Millisecond), func() {
		distr.Socket.Close()
	})

	// Run the distributor node.
	_ = distr.Run()
}
