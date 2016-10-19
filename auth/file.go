package auth

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Structs

// FileAuthenticator contains file based authentication
// information including the in-memory map of username to
// password mapping.
type FileAuthenticator struct {
	File      string
	Separator string
	Users     map[string]string
}

// Functions

// NewFileAuthenticator takes in a file name and a separator,
// reads in specified file and parses it line by line as
// username - password elements separated by the separator.
// At the end, the returned struct contains the information
// and an in-memory map of username mapped to password.
func NewFileAuthenticator(file string, sep string) (*FileAuthenticator, error) {

	var err error
	var handle *os.File

	// Reserve space for users maps in memory.
	Users := make(map[string]string)

	// Open file with authentication information.
	handle, err = os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("[auth.NewFileAuthenticator] Could not open supplied authentication file: %s\n", err.Error())
	}
	defer handle.Close()

	// Create a new scanner on top of file handle.
	scanner := bufio.NewScanner(handle)

	// As long as there are lines left, scan them into memory.
	for scanner.Scan() {

		// Split read line based on separator defined in config file.
		userData := strings.Split(scanner.Text(), sep)

		// Set username key in users map to password.
		Users[userData[0]] = userData[1]
	}

	// If the scanner ended with an error, report it.
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("[auth.NewFileAuthenticator] Experienced error while scanning authentication file: %s\n", err.Error())
	}

	return &FileAuthenticator{
		File:      file,
		Separator: sep,
		Users:     Users,
	}, nil
}

// AuthenticatePlain performs the actual authentication
// process by taking supplied credentials and attempting
// to find a matching entry the in-memory table taken from
// the authentication file.
func (f *FileAuthenticator) AuthenticatePlain(username string, password string) error {

	pass, ok := f.Users[username]

	// Check if username is present in map.
	if !ok {
		return fmt.Errorf("Username not found in map")
	}

	// Check if passwords match.
	if pass != password {
		return fmt.Errorf("Passwords did not match")
	}

	return nil
}
