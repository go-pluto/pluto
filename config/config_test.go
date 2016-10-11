package config_test

import (
	"testing"

	"github.com/numbleroot/pluto/config"
)

// Functions

// TestLoadConfig executes a black-box test on the
// implemented functionalities to load a TOML config file.
func TestLoadConfig(t *testing.T) {

	// Try to load a broken config file. This should fail.
	_, err := config.LoadConfig("broken-config.toml")
	if err == nil {
		t.Fatal("[config.TestLoadConfig] Expected fail while loading broken-config.toml but received 'nil' error.")
	}

	// Now load a valid config.
	config, err := config.LoadConfig("config.toml")
	if err != nil {
		t.Fatalf("[config.TestLoadConfig] Expected success while loading config.toml but received: '%s'\n", err.Error())
	}

	// Check for test success.
	if config.TLS.CertLoc != "/very/complicated/test/directory/certificate.test" {
		t.Fatalf("[config.TestLoadConfig] Expected '%s' but received '%s'\n", "/very/complicated/test/directory/certificate.test", config.TLS.CertLoc)
	}
}
