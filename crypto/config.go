package crypto

import (
	"fmt"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

// Functions

// NewPublicTLSConfig returns a TLS config that is to be used
// when exposing ports to the public Internet. It defines very
// strict defaults but assumes that available system cert pools
// will be used when verifying certificates.
func NewPublicTLSConfig(certPath string, keyPath string) (*tls.Config, error) {

	// Define very strict defaults for public TLS usage.
	// Good parts of them were taken from the excellent post:
	// "Achieving a Perfect SSL Labs Score with Go":
	// https://blog.bracelab.com/achieving-perfect-ssl-labs-score-with-go
	// With further optimizations for speed and security from here:
	// "So you want to expose Go on the Internet"
	// https://blog.gopheracademy.com/advent-2016/exposing-go-on-the-internet/
	config := &tls.Config{
		Certificates:             make([]tls.Certificate, 1),
		InsecureSkipVerify:       false,
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	// Put certificate specified via arguments as the
	// only certificate into config.
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("[crypto.NewPublicTLSConfig] Failed to load TLS cert and key: %v", err)
	}
	config.Certificates[0] = cert

	// Build Common Name (CN) and Subject Alternate
	// Name (SAN) from supplied certificate.
	config.BuildNameToCertificate()

	return config, nil
}

// NewInternalTLSConfig returns a TLS config that is
// already configured completely for use in nodes to
// communicate internally. It defines very strict defaults
// and requires all nodes to verify each other by TLS means.
func NewInternalTLSConfig(certPath string, keyPath string, rootCertPath string) (*tls.Config, error) {

	// Define very strict defaults for internal TLS usage.
	// Good parts of them were taken from the excellent post:
	// "Achieving a Perfect SSL Labs Score with Go":
	// https://blog.bracelab.com/achieving-perfect-ssl-labs-score-with-go
	// With further optimizations for speed and security from here:
	// "So you want to expose Go on the Internet"
	// https://blog.gopheracademy.com/advent-2016/exposing-go-on-the-internet/
	config := &tls.Config{
		RootCAs:                  x509.NewCertPool(),
		ClientCAs:                x509.NewCertPool(),
		ClientAuth:               tls.RequireAndVerifyClientCert,
		Certificates:             make([]tls.Certificate, 1),
		InsecureSkipVerify:       false,
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP256},
		PreferServerCipherSuites: true,
		SessionTicketsDisabled:   false,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	// Read in root certificate in PEM format supplied
	// via path in arguments.
	rootCert, err := ioutil.ReadFile(rootCertPath)
	if err != nil {
		return nil, fmt.Errorf("[crypto.NewInternalTLSConfig] Reading root certificate into memory failed with: %v", err)
	}

	// Append root certificate to root CA pool.
	if ok := config.RootCAs.AppendCertsFromPEM(rootCert); !ok {
		return nil, fmt.Errorf("[crypto.NewInternalTLSConfig] Failed to append root certificate to root CA pool: %v", err)
	}

	// Append root certificate to client CA pool.
	if ok := config.ClientCAs.AppendCertsFromPEM(rootCert); !ok {
		return nil, fmt.Errorf("[crypto.NewInternalTLSConfig] Failed to append root certificate to client CA pool: %v", err)
	}

	// Put certificate specified via arguments as the
	// only certificate into config.
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("[crypto.NewInternalTLSConfig] Failed to load TLS cert and key: %v", err)
	}
	config.Certificates[0] = cert

	// Build Common Name (CN) and Subject Alternate
	// Name (SAN) from supplied certificate.
	config.BuildNameToCertificate()

	return config, nil
}
