package config_test

import (
	"testing"

	"path/filepath"

	"github.com/numbleroot/pluto/config"
)

// Functions

// TestLoadConfig executes a black-box test on the
// implemented functionalities to load a TOML config file.
func TestLoadConfig(t *testing.T) {

	// Try to load a broken config file. This should fail.
	_, err := config.LoadConfig("test-broken-config.toml")
	if err == nil {
		t.Fatal("[config.TestLoadConfig] Expected fail while loading 'test-broken-config.toml' but received 'nil' error.")
	}

	// Now load a valid config.
	conf, err := config.LoadConfig("../test-config.toml")
	if err != nil {
		t.Fatalf("[config.TestLoadConfig] Expected success while loading 'test-config.toml' but received: '%s'\n", err.Error())
	}

	// Retrieve absolute path of pluto directory.
	absPlutoPath, err := filepath.Abs("../")
	if err != nil {
		t.Fatalf("[config.TestLoadConfig] Expected to retrieve absolute path of pluto directory with success but error says: %s\n", err.Error())
	}

	// Build correct cert location path.
	absCertLoc := filepath.Join(absPlutoPath, "private/public-distributor-cert.pem")

	// Check for test success.
	if conf.Distributor.PublicTLS.CertLoc != absCertLoc {
		t.Fatalf("[config.TestLoadConfig] Expected '%s' but received '%s'\n", absCertLoc, conf.Distributor.PublicTLS.CertLoc)
	}
}
