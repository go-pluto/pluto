package config_test

import (
	"testing"

	"github.com/numbleroot/pluto/config"
)

// Functions

// TestLoadEnv executes a black-box test on the
// implemented functionalities to load a .env file.
func TestLoadEnv(t *testing.T) {

	// Execute main function.
	env := config.LoadEnv()

	// Check for test success.
	if env.Secret != "works" {
		t.Fatalf("[config.TestLoadEnv] Expected '%s' but received '%s'\n", "works", env.Secret)
	}
}
