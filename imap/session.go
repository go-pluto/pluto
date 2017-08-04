package imap

import (
	"github.com/go-pluto/maildir"
	"github.com/go-pluto/pluto/comm"
)

// Constants

// Integer counter for IMAP states.
const (
	StateAny State = iota
	StateNotAuthenticated
	StateAuthenticated
	StateMailbox
	StateLogout
)

// Structs

// State represents the integer value associated with one
// of the implemented IMAP states a connection can be in.
type State int

// Session contains all elements needed for tracking
// and performing the actual IMAP operations for an
// authenticated client.
type Session struct {
	State             State
	ClientID          string
	UserName          string
	RespWorker        string
	StorageSubnetChan chan comm.Msg
	SelectedMailbox   string
	AppendInProg      *AppendInProg
}

// AppendInProg captures the important environment
// characteristics handed from AppendBegin to AppendEnd.
type AppendInProg struct {
	Tag         string
	Mailbox     string
	Maildir     maildir.Dir
	FlagsRaw    string
	DateTimeRaw string
}
