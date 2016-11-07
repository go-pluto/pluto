package auth

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/utils"
)

// Structs

// FileAuthenticator contains file based authentication
// information including the in-memory map of username to
// password mapping.
type FileAuthenticator struct {
	lock      sync.Mutex
	File      string
	Separator string
	Users     []User
}

// User holds name and password from one line from users file.
type User struct {
	ID       int
	Name     string
	Password string
	Token    string
}

// UsersByName defines a list type of users to search efficiently.
type UsersByName []User

// Functions

// Make list of users searchable efficiently.
func (u UsersByName) Len() int           { return len(u) }
func (u UsersByName) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u UsersByName) Less(i, j int) bool { return u[i].Name < u[j].Name }

// NewFileAuthenticator takes in a file name and a separator,
// reads in specified file and parses it line by line as
// username - password elements separated by the separator.
// At the end, the returned struct contains the information
// and an in-memory map of username mapped to password.
func NewFileAuthenticator(file string, sep string) (*FileAuthenticator, error) {

	i := 1
	var err error
	var handle *os.File
	var nextUser User

	// Reserve space for the ordered users list in memory.
	Users := make([]User, 0, 50)

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

		// Create new user struct.
		nextUser = User{
			ID:       i,
			Name:     userData[0],
			Password: userData[1],
			Token:    "",
		}

		// Append new user element to slice.
		Users = append(Users, nextUser)

		// Increment original ID counter.
		i++
	}

	// If the scanner ended with an error, report it.
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("[auth.NewFileAuthenticator] Experienced error while scanning authentication file: %s\n", err.Error())
	}

	// Sort users list to search it efficiently later on.
	sort.Sort(UsersByName(Users))

	return &FileAuthenticator{
		lock:      sync.Mutex{},
		File:      file,
		Separator: sep,
		Users:     Users,
	}, nil
}

// GetOriginalIDOfUser finds position of supplied user in users
// list. It is assumed that existence check was already performed,
// for example via AuthenticatePlain.
func (f *FileAuthenticator) GetOriginalIDOfUser(username string) int {

	// This routine has to be safe for concurrent usage,
	// therefore lock the struct on entry.
	f.lock.Lock()
	defer f.lock.Unlock()

	// Search in user list for user matching supplied name.
	i := sort.Search(len(f.Users), func(i int) bool {
		return f.Users[i].Name >= username
	})

	return f.Users[i].ID
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

// DeleteTokenForUser removes the token from the in-memory
// user list that marks an active session of an user.
func (f *FileAuthenticator) DeleteTokenForUser(username string) {

	// This routine has to be safe for concurrent usage,
	// therefore lock the struct on entry.
	f.lock.Lock()
	defer f.lock.Unlock()

	// Search in user list for user matching supplied name.
	i := sort.Search(len(f.Users), func(i int) bool {
		return f.Users[i].Name >= username
	})

	// Remove the token if present.
	if f.Users[i].Token != "" {
		f.Users[i].Token = ""
	}
}

// AuthenticatePlain performs the actual authentication
// process by taking supplied credentials and attempting
// to find a matching entry the in-memory list taken from
// the authentication file.
func (f *FileAuthenticator) AuthenticatePlain(username string, password string) (int, string, error) {

	// This routine has to be safe for concurrent usage,
	// therefore lock the struct on entry.
	f.lock.Lock()
	defer f.lock.Unlock()

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

	// Generate a random session token and save it
	// in user's entry of in-memory list.
	f.Users[i].Token = utils.GenerateRandomString(16)

	return f.Users[i].ID, f.Users[i].Token, nil
}
