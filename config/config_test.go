package config_test

import (
	"testing"

	"path/filepath"

	"github.com/go-pluto/pluto/config"
	"github.com/stretchr/testify/assert"
)

// Functions

// TestLoadConfig executes a black-box test on the
// implemented functionalities to load a TOML config file.
func TestLoadConfig(t *testing.T) {

	// Try to load a broken config file. This should fail.
	_, err := config.LoadConfig("test-broken-config.toml")
	assert.NotNilf(t, err, "expected LoadConfig() to return non-nil error while loading 'test-broken-config.toml' but error was nil")

	// Now load a valid config.
	conf, err := config.LoadConfig("../test-config.toml")
	assert.Nilf(t, err, "expected LoadConfig() to return nil error while loading valid config but received: %v", err)

	// Retrieve absolute path of pluto directory.
	absPlutoPath, err := filepath.Abs("../")
	assert.Nilf(t, err, "expected to find absolute pluto path with nil error but received: %v", err)

	// Build correct cert location path.
	absCertLoc := filepath.Join(absPlutoPath, "private/public-distributor-cert.pem")

	// Check for test success.
	assert.Equalf(t, absCertLoc, conf.Distributor.PublicCertLoc, "expected certificate path to be '%s' but found '%s'", absCertLoc, conf.Distributor.PublicCertLoc)
}
