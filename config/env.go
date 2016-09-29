package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/numbleroot/pluto/types"
)

// Functions

// LoadEnv looks for an .env file in the directory
// of pluto and reads in all defined values.
func LoadEnv() *types.Env {

	// Load environment file.
	err := godotenv.Load()
	if err != nil {
		log.Fatal("[config.LoadEnv] Failed to read in .env file with: %s\n", err)
	}

	env := new(types.Env)

	// Fill variables from .env into struct.
	env.Secret = os.Getenv("SECRET")

	return env
}
