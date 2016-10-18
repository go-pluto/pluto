package config_test

import (
	"os"
	"testing"

	"github.com/numbleroot/pluto/config"
)

// Functions

// TestLoadEnv executes a black-box test on the
// implemented functionalities to load a .env file.
func TestLoadEnv(t *testing.T) {

	var err error

	// Rename provided .env file to .hidden.
	err = os.Rename(".env", ".hidden")
	if err != nil {
		t.Errorf("[config.TestLoadEnv] Encountered error while renaming file '.env' => '.hidden': '%s'\n", err.Error())
	}

	// Try to load non-existent .env file.
	err = config.LoadEnv()
	if err == nil {
		t.Fatalf("[config.TestLoadEnv] Expected fail while loading non-existent '.env' but received 'nil' error.")
	}

	// Rename .env file back to original name.
	err = os.Rename(".hidden", ".env")
	if err != nil {
		t.Errorf("[config.TestLoadEnv] Encountered error while renaming file back '.hidden' => '.env': '%s'\n", err.Error())
	}

	// Load an existing, valid .env file.
	err = config.LoadEnv()
	if err != nil {
		t.Fatalf("[config.TestLoadEnv] Expected success while loading '.env' but received: '%s'\n", err.Error())
	}
}
