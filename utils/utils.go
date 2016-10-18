package utils

import (
	"log"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/numbleroot/pluto/config"
)

// Functions

// CreateTestEnv initializes the needed environment
// for performing various tests against parts of
// the pluto system.
func CreateTestEnv() (*config.Config, *tls.Config) {

	var err error
	var Config *config.Config
	var TLSConfig *tls.Config

	// Read configuration from file.
	Config, err = config.LoadConfig("test-config.toml")
	if err != nil {
		log.Fatalf("[imap.testEnv] Failed to load config file with: '%s'\n", err.Error())
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

	// Read distributor certificate specified in pluto's main
	// config file into memory.
	rootCert, err := ioutil.ReadFile(Config.Distributor.TLS.CertLoc)
	if err != nil {
		log.Fatalf("[imap.testEnv] Reading distributor certificate into memory failed with: %s\n", err.Error())
	}

	// Append certificate in PEM form to pool.
	ok := rootCerts.AppendCertsFromPEM(rootCert)
	if !ok {
		log.Fatalf("[imap.testEnv] Failed to append certificate to pool: %s\n", err.Error())
	}

	// Now make created pool the root pool
	// of above global TLS config.
	TLSConfig.RootCAs = rootCerts

	return Config, TLSConfig
}
