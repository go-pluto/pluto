package config

import (
	"fmt"

	"github.com/joho/godotenv"
)

// Functions

// LoadEnv looks for an .env file in the directory
// of pluto and reads in all defined values.
func LoadEnv() error {

	// Load environment file.
	err := godotenv.Load(".env")
	if err != nil {
		return fmt.Errorf("[config.LoadEnv] Failed to read in .env file with: %s\n", err.Error())
	}

	return nil
}
