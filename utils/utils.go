package utils

import (
	"fmt"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/go-pluto/pluto/config"
	"github.com/go-pluto/pluto/crypto"
)

// Structs

// TestEnv carries everything needed for a full
// grown test of pluto with multiple nodes.
type TestEnv struct {
	Config      *config.Config
	TLSConfig   *tls.Config
	Addr        string
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
func CreateTestEnv(configFilePath string) (*TestEnv, error) {

	// Read configuration from file.
	config, err := config.LoadConfig(configFilePath)
	if err != nil {
		return nil, err
	}

	// Create public TLS config (distributor).
	tlsConfig, err := crypto.NewPublicTLSConfig(config.Distributor.PublicCertLoc, config.Distributor.PublicKeyLoc)
	if err != nil {
		return nil, err
	}

	// For tests, we currently need to build a custom
	// x509 cert pool to accept the self-signed public
	// distributor certificate.
	tlsConfig.RootCAs = x509.NewCertPool()

	// Read distributor's public certificate in PEM format
	// specified in pluto's main config file into memory.
	rootCert, err := ioutil.ReadFile(config.Distributor.PublicCertLoc)
	if err != nil {
		return nil, fmt.Errorf("reading distributor's public certificate into memory failed with: %v", err)
	}

	// Append certificate to test client's root CA pool.
	if ok := tlsConfig.RootCAs.AppendCertsFromPEM(rootCert); !ok {
		return nil, fmt.Errorf("failed to append certificate to pool: %v", err)
	}

	// Return properly initilized and complete struct
	// representing a test environment.
	return &TestEnv{
		Config:      config,
		TLSConfig:   tlsConfig,
		Addr:        config.Distributor.PublicMailAddr,
		DownDistr:   make(chan struct{}),
		DownWorker:  make(chan struct{}),
		DownStorage: make(chan struct{}),
		DoneDistr:   make(chan struct{}),
		DoneWorker:  make(chan struct{}),
		DoneStorage: make(chan struct{}),
	}, nil
}
