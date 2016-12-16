package imap

import (
	"fmt"
	"strings"

	"path/filepath"
)

// Structs

// Context carries session-identifying information sent
// from distributor to worker node.
type Context struct {
	ClientID        string
	IMAPState       IMAPState
	UserName        string
	UserCRDTPath    string
	UserMaildirPath string
	SelectedMailbox string
}

// Functions

// UpdateClientContext takes in received raw clientID string,
// verifies, parses it, checks for existing client context and
// if successful, returns the clientID. Worker version.
func (worker *Worker) UpdateClientContext(clientIDRaw string) (string, error) {

	// Split received clientID string at white spaces
	// and check for correct amount of found fields.
	fields := strings.Fields(clientIDRaw)
	if len(fields) != 4 {
		return "", fmt.Errorf("received an invalid clientID information")
	}

	// Check if structure is correct.
	if fields[0] != ">" || fields[1] != "id:" || fields[3] != "<" {
		return "", fmt.Errorf("received an invalid clientID information")
	}

	// Save clientID for later use.
	clientID := fields[2]

	// Parse parts including user name from clientID.
	clientInfo := strings.SplitN(fields[2], ":", 3)

	// This routine has to be safe for concurrent usage,
	// therefore lock the struct on entry.
	worker.lock.Lock()

	// Check if for parsed clientID a session is already existing.
	if _, found := worker.Contexts[clientID]; !found {

		worker.Contexts[clientID] = &Context{
			ClientID:        clientID,
			IMAPState:       AUTHENTICATED,
			UserName:        clientInfo[2],
			UserCRDTPath:    filepath.Join(worker.Config.Workers[worker.Name].CRDTLayerRoot, clientInfo[2]),
			UserMaildirPath: filepath.Join(worker.Config.Workers[worker.Name].MaildirRoot, clientInfo[2]),
		}
	}

	worker.lock.Unlock()

	// Return extracted clientID.
	return clientID, nil
}

func (failWorker *FailoverWorker) ExtractClientContext(clientIDRaw string) (string, error) {

	// Split received clientID string at white spaces
	// and check for correct amount of found fields.
	fields := strings.Fields(clientIDRaw)
	if len(fields) != 4 {
		return "", fmt.Errorf("received an invalid clientID information")
	}

	// Check if structure is correct.
	if fields[0] != ">" || fields[1] != "id:" || fields[3] != "<" {
		return "", fmt.Errorf("received an invalid clientID information")
	}

	// Return extracted clientID.
	return fields[2], nil
}

// UpdateClientContext takes in received raw clientID string,
// verifies, parses it, checks for existing client context and
// if successful, returns the clientID. Storage version.
func (storage *Storage) UpdateClientContext(clientIDRaw string) (string, string, error) {

	// Split received clientID string at white spaces
	// and check for correct amount of found fields.
	fields := strings.Fields(clientIDRaw)
	if len(fields) != 5 {
		return "", "", fmt.Errorf("received an invalid clientID information")
	}

	// Check if structure is correct.
	if fields[0] != ">" || fields[1] != "id:" || fields[4] != "<" {
		return "", "", fmt.Errorf("received an invalid clientID information")
	}

	// Save clientID for later use.
	clientID := fields[2]

	// Parse parts including user name from clientID.
	clientInfo := strings.SplitN(fields[2], ":", 3)

	// This routine has to be safe for concurrent usage,
	// therefore lock the struct on entry.
	storage.lock.Lock()

	// Check if for parsed clientID a session is already existing.
	if _, found := storage.Contexts[clientID]; !found {

		storage.Contexts[clientID] = &Context{
			ClientID:        clientID,
			IMAPState:       AUTHENTICATED,
			UserName:        clientInfo[2],
			UserCRDTPath:    filepath.Join(storage.Config.Storage.CRDTLayerRoot, clientInfo[2]),
			UserMaildirPath: filepath.Join(storage.Config.Storage.MaildirRoot, clientInfo[2]),
		}
	}

	storage.lock.Unlock()

	// Return extracted clientID and name of originally
	// responsible worker node.
	return clientID, fields[3], nil
}
