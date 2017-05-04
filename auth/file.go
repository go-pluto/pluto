package auth

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/numbleroot/pluto/config"
)

// Structs

// FileAuthenticator contains file based authentication
// information including the in-memory map of username to
// password mapping.
type FileAuthenticator struct {
	Users []User
}

// User holds name and password from one line from users file.
type User struct {
	ID       int
	Name     string
	Password string
}

// Functions

// NewFileAuthenticator takes in a file name and a separator,
// reads in specified file and parses it line by line as
// username - password elements separated by the separator.
// At the end, the returned struct contains the information
// and an in-memory map of username mapped to password.
func NewFileAuthenticator(file string, sep string) (*FileAuthenticator, error) {

	// Reserve space for the ordered users list in memory.
	users := make([]User, 0, 50)

	// Open file with authentication information.
	handle, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("[auth.NewFileAuthenticator] Could not open supplied authentication file: %v", err)
	}
	defer handle.Close()

	// Create a new scanner on top of file handle.
	scanner := bufio.NewScanner(handle)

	i := 1
	// As long as there are lines left, scan them into memory.
	for scanner.Scan() {

		// Split read line based on separator defined in config file.
		userData := strings.Split(scanner.Text(), sep)

		// Create new user struct.
		nextUser := User{
			ID:       i,
			Name:     userData[0],
			Password: userData[1],
		}

		// Append new user element to slice.
		users = append(users, nextUser)

		// Increment original ID counter.
		i++
	}

	// If the scanner ended with an error, report it.
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("[auth.NewFileAuthenticator] Experienced error while scanning authentication file: %v", err)
	}

	// Sort users list to search it efficiently later on.
	sort.Slice(users, func(i, j int) bool {
		return users[i].Name < users[j].Name
	})

	return &FileAuthenticator{
		Users: users,
	}, nil
}

// GetWorkerForUser returns the name of the worker node
// that is responsible for handling the user's mailbox.
func (f *FileAuthenticator) GetWorkerForUser(workers map[string]config.Worker, id int) (string, error) {

	for name, worker := range workers {

		// Range over all available workers and see which worker
		// is responsible for the range of user IDs that contains
		// the supplied user ID.
		if id >= worker.UserStart && id <= worker.UserEnd {
			return name, nil
		}
	}

	return "", fmt.Errorf("no worker responsible for user ID %d", id)
}

// AuthenticatePlain performs the actual authentication
// process by taking supplied credentials and attempting
// to find a matching entry the in-memory list taken from
// the authentication file.
func (f *FileAuthenticator) AuthenticatePlain(username string, password string, clientAddr string) (int, string, error) {

	// Search in user list for user matching supplied name.
	i := sort.Search(len(f.Users), func(i int) bool {
		return f.Users[i].Name >= username
	})

	// If that user does not exist, throw an error.
	if !((i < len(f.Users)) && (f.Users[i].Name == username)) {
		return -1, "", fmt.Errorf("username not found in list of users")
	}

	// Check if passwords match.
	if f.Users[i].Password != password {
		return -1, "", fmt.Errorf("passwords did not match")
	}

	// Build the deterministic client-specific session identifier.
	clientID := fmt.Sprintf("%s:%s", clientAddr, username)

	return f.Users[i].ID, clientID, nil
}
