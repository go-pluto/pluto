package utils

import (
	"fmt"
	"time"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"math/rand"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
)

// Constants

const (
	charBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charIdxBits = 6
	charIdxMask = (1 << charIdxBits) - 1
	charIdxMax  = 63 / charIdxBits
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

// GenerateRandomString returns a string of random
// alphanumerical characters of length n.
// Kudos to author icza, see his incredible post:
// http://stackoverflow.com/a/31832326
func GenerateRandomString(n int) string {

	b := make([]byte, n)
	src := rand.NewSource(time.Now().UnixNano())

	// A src.Int63() generates 63 random bits, enough for charIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), charIdxMax; i >= 0; {

		if remain == 0 {
			cache, remain = src.Int63(), charIdxMax
		}

		if idx := int(cache & charIdxMask); idx < len(charBytes) {
			b[i] = charBytes[idx]
			i--
		}

		cache >>= charIdxBits
		remain--
	}

	return string(b)
}
