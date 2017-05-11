// +build ignore

// Go script to generate PKI infrastructure for pluto.
// Heavily inspired by:
// - https://raw.githubusercontent.com/golang/go/master/src/crypto/tls/generate_cert.go
// - https://ericchiang.github.io/tls/go/https/2015/06/21/go-tls.html
package main

import (
	"flag"
	"fmt"
	stdlog "log"
	"net"
	"os"
	"time"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"

	"github.com/numbleroot/pluto/config"
)

// Variables

var (
	pathPrefix  = flag.String("path-prefix", "../", "If you are running this script from somewhere else than its folder, specify a different prefix for each path used later on")
	plutoConfig = flag.String("pluto-config", "config.toml", "If you use a custom config path specify it via this flag")
	validFrom   = flag.String("start-date", "", "Creation date formatted as Jan 1 15:04:05 2011")
	validFor    = flag.Duration("duration", (90 * 24 * time.Hour), "Duration that certificates will be valid for")
	rsaBits     = flag.Int("rsa-bits", 2048, "Size of RSA keys to generate")
)

// Functions

// BootstrapCertTempl returns a certificate template that
// has all default values for our certificates already set.
func BootstrapCertTempl(nBef time.Time, nAft time.Time) (*x509.Certificate, error) {

	// For serial number generation we need a biggest
	// number to mark the range of the serial number.
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)

	// Now generate that random number.
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("[crypto.GeneratePKI] Could not generate random serial number: %s\n", err.Error())
	}

	// Build a default template we use for each certificate.
	certificateTemplate := &x509.Certificate{
		SignatureAlgorithm:    x509.SHA512WithRSA,
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{Organization: []string{"Pluto's internal PKI"}},
		NotBefore:             nBef,
		NotAfter:              nAft,
		BasicConstraintsValid: true,
	}

	return certificateTemplate, nil
}

// CreateNodeCert performs all needed actions in order
// to obtain a node's key pair and certificate signed by
// the root certificate.
func CreateNodeCert(pathPrefix string, fileName string, rsaBits int, nBef time.Time, nAft time.Time, ipAdresses []string, rootCert *x509.Certificate, rootKey *rsa.PrivateKey) error {

	stdlog.Printf("[crypto.GeneratePKI] === Generating for %s ===", fileName)

	// Generate this node's key pair.
	key, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return fmt.Errorf("[crypto.GeneratePKI] Failed to generate key for %s: %s\n", fileName, err.Error())
	}

	// Fetch a new certificate template.
	template, err := BootstrapCertTempl(nBef, nAft)
	if err != nil {
		return err
	}

	// Set specific certificate values for a normal node certificate.
	template.KeyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

	for _, ipAddr := range ipAdresses {
		// Parse supplied IP addresses and append valid results.
		if ip := net.ParseIP(ipAddr); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		}
	}

	// Create the actual node certificate.
	certDER, err := x509.CreateCertificate(rand.Reader, template, rootCert, &key.PublicKey, rootKey)
	if err != nil {
		return fmt.Errorf("[crypto.GeneratePKI] Failed to create DER byte representation of certificate for %s: %s\n", fileName, err.Error())
	}

	// Open file handle to store node certificate at.
	certFile, err := os.Create(fmt.Sprintf("%sprivate/%s-cert.pem", pathPrefix, fileName))
	if err != nil {
		return fmt.Errorf("[crypto.GeneratePKI] Failed to open file for certificate for %s: %s\n", fileName, err.Error())
	}
	defer certFile.Close()

	// Encode it in PEM format and save to disk.
	err = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err != nil {
		return fmt.Errorf("[crypto.GeneratePKI] Failed to write certificate for %s in PEM format to disk: %s\n", fileName, err.Error())
	}
	certFile.Sync()

	stdlog.Printf("[crypto.GeneratePKI] Saved %s-cert.pem to disk.", fileName)

	// Additionally, open file handle for node key.
	keyFile, err := os.OpenFile(fmt.Sprintf("%sprivate/%s-key.pem", pathPrefix, fileName), (os.O_WRONLY | os.O_CREATE | os.O_TRUNC), 0600)
	if err != nil {
		return fmt.Errorf("[crypto.GeneratePKI] Failed to open file for key for %s: %s\n", fileName, err.Error())
	}

	// Encode it in PEM format and save to disk.
	err = pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err != nil {
		return fmt.Errorf("[crypto.GeneratePKI] Failed to write key for %s in PEM format to disk: %s\n", fileName, err.Error())
	}
	keyFile.Sync()

	stdlog.Printf("[crypto.GeneratePKI] Saved %s-key.pem to disk.", fileName)
	stdlog.Printf("[crypto.GeneratePKI] === Done generating for %s ===", fileName)

	return nil
}

func main() {

	var err error
	var notBefore time.Time
	var notAfter time.Time

	// Parse supplied command-line flags.
	flag.Parse()

	stdlog.Println("[crypto.GeneratePKI] Building pluto's internal PKI.")

	if len(*validFrom) == 0 {

		// If no start date supplied, assume now.
		notBefore = time.Now()
	} else {

		// If start date supplied, try to parse.
		notBefore, err = time.Parse("Jan 2 15:04:05 2006", *validFrom)
		if err != nil {
			stdlog.Fatalf("[crypto.GeneratePKI] Failed to parse creation date of certificates: %s", err.Error())
		}
	}

	// Add life-time of certificates to creation date.
	notAfter = notBefore.Add(*validFor)

	// Load pluto config.
	config, err := config.LoadConfig(fmt.Sprintf("%s%s", *pathPrefix, *plutoConfig))
	if err != nil {
		stdlog.Fatal(err)
	}

	stdlog.Println("[crypto.GeneratePKI] === Generating root certificate ===")

	// Generate root key pair.
	rootKey, err := rsa.GenerateKey(rand.Reader, *rsaBits)
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to generate root key: %s", err.Error())
	}

	// Prepare to create the root certificate which will
	// be used to sign internally used certificates.
	rootTemplate, err := BootstrapCertTempl(notBefore, notAfter)
	if err != nil {
		stdlog.Fatal(err)
	}

	// Set specific certificate values for a root certificate.
	rootTemplate.IsCA = true
	rootTemplate.KeyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign
	rootTemplate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

	// Create the actual root certificate.
	rootCertDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to create DER byte representation of root certificate: %s", err.Error())
	}

	// Parse root certificate again so that we can sign with it.
	rootCert, err := x509.ParseCertificate(rootCertDER)
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to parse DER root certificate to x509 certificate: %s", err.Error())
	}

	// Open file handle to store root certificate at.
	rootCertFile, err := os.Create(fmt.Sprintf("%sprivate/root-cert.pem", *pathPrefix))
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to open file for root certificate: %s", err.Error())
	}
	defer rootCertFile.Close()

	// Encode it in PEM format and save to disk.
	err = pem.Encode(rootCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: rootCertDER})
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to write root certificate in PEM format to disk: %s", err.Error())
	}
	rootCertFile.Sync()

	stdlog.Println("[crypto.GeneratePKI] Saved root-cert.pem to disk.")

	// Additionally, open file handle for root key.
	rootKeyFile, err := os.OpenFile(fmt.Sprintf("%sprivate/root-key.pem", *pathPrefix), (os.O_WRONLY | os.O_CREATE | os.O_TRUNC), 0600)
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to open file for root key: %s", err.Error())
	}

	// Encode it in PEM format and save to disk.
	err = pem.Encode(rootKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rootKey)})
	if err != nil {
		stdlog.Fatalf("[crypto.GeneratePKI] Failed to write root key in PEM format to disk: %s", err.Error())
	}
	rootKeyFile.Sync()

	stdlog.Println("[crypto.GeneratePKI] Saved root-key.pem to disk.")
	stdlog.Println("[crypto.GeneratePKI] === Done generating root certificate ===")

	// Generate distributor's internal key and signed certificate.
	err = CreateNodeCert(*pathPrefix, "internal-distributor", *rsaBits, notBefore, notAfter, []string{config.Distributor.PublicIP}, rootCert, rootKey)
	if err != nil {
		stdlog.Fatal(err)
	}

	for name, worker := range config.Workers {

		// For each worker node, generate an internal key pair
		// and a signed certificate.
		err = CreateNodeCert(*pathPrefix, fmt.Sprintf("internal-%s", name), *rsaBits, notBefore, notAfter, []string{worker.PublicIP}, rootCert, rootKey)
		if err != nil {
			stdlog.Fatal(err)
		}
	}

	// Generate the storage's internal key pair
	// and signed certificate.
	err = CreateNodeCert(*pathPrefix, "internal-storage", *rsaBits, notBefore, notAfter, []string{config.Storage.PublicIP}, rootCert, rootKey)
	if err != nil {
		stdlog.Fatal(err)
	}

	stdlog.Println("[crypto.GeneratePKI] Done building pluto's internal PKI. Goodbye.")
}
