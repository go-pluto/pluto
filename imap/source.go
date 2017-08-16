package imap

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"io/ioutil"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-pluto/maildir"
	"github.com/go-pluto/pluto/comm"
	"github.com/go-pluto/pluto/crdt"
)

// Structs

// Mailbox represents the state of one user's
// mailbox in the provided email service. It
// serializes access for mutating state, contains
// the structure OR-Set, keeps track of message
// sequence numbers, and provides user-specific
// path values in the file system.
type Mailbox struct {
	Logger             log.Logger
	Lock               *sync.RWMutex
	Structure          *crdt.ORSet
	Mails              map[string][]string
	CRDTPath           string
	MaildirPath        string
	HierarchySeparator string
}

// Functions

// Select sets the current mailbox based on supplied
// payload to user-instructed value. A return value of
// this function does not indicate whether the command
// was successfully handled according to IMAP semantics,
// but rather whether a fatal error occurred or a complete
// answer could been sent. So, in case of an user error
// (e.g. a missing mailbox to select) but otherwise correct
// handling, this function would send a useful message to
// the client and still return true.
func (mailbox *Mailbox) Select(s *Session, req *Request, syncChan chan comm.Msg) (*Reply, error) {

	if (s.State != StateAuthenticated) && (s.State != StateMailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command SELECT cannot be executed in this state", req.Tag),
		}, nil
	}

	if len(req.Payload) < 1 {

		// If no mailbox to select was specified in payload,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command SELECT was sent without a mailbox to select", req.Tag),
		}, nil
	}

	// Split payload on every whitespace character.
	reqMailboxes := strings.Split(req.Payload, " ")

	if len(reqMailboxes) != 1 {

		// If there were more than two names supplied to select,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command SELECT was sent with multiple mailbox names instead of only one", req.Tag),
		}, nil
	}

	reqMailboxPath := mailbox.MaildirPath

	// If any other mailbox than INBOX was specified,
	// append it to mailbox in order to check it.
	if reqMailboxes[0] != "INBOX" {
		reqMailboxPath = filepath.Join(reqMailboxPath, reqMailboxes[0])
	}

	reqMailbox := maildir.Dir(reqMailboxPath)

	// Check if mailbox is existing and a correct maildir folder.
	err := reqMailbox.Check()
	if err != nil {

		// If specified maildir did not turn out to be a valid one,
		// this is a client error. Return NO statement.
		return &Reply{
			Text: fmt.Sprintf("%s NO SELECT failure, not a valid Maildir folder", req.Tag),
		}, nil
	}

	// Set selected mailbox in connection to supplied one
	// and advance IMAP state of connection to Mailbox.
	s.State = StateMailbox
	s.SelectedMailbox = reqMailboxes[0]

	mailbox.Lock.RLock()
	defer mailbox.Lock.RUnlock()

	// Count how many mails do not have the \Seen
	// flag attached, i.e. recent mails.
	recentMails := 0
	for _, mail := range mailbox.Mails[s.SelectedMailbox] {

		// Retrieve flags of mail.
		mailFlags, err := reqMailbox.Flags(mail, false)
		if err != nil {

			// Return an vague explanation error to caller in
			// error case and indicate failure to distributor.
			return &Reply{
				Text:   "* BAD Internal server error, sorry. Closing connection.",
				Status: 1,
			}, fmt.Errorf("error while retrieving flags for mail: %v", err)
		}

		// Check if \Seen flag is present.
		if strings.ContainsRune(mailFlags, 'S') != true {
			recentMails++
		}
	}

	// TODO: Add other SELECT response elements if needed.

	// Send answer to requesting client.
	return &Reply{
		Text: fmt.Sprintf("* %d EXISTS\r\n* %d RECENT\r\n* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)]\r\n%s OK [READ-WRITE] SELECT completed", len(mailbox.Mails), recentMails, req.Tag),
	}, nil
}

// Create attempts to create a mailbox folder with
// the name taken from the payload of the request.
func (mailbox *Mailbox) Create(s *Session, req *Request, syncChan chan comm.Msg) (*Reply, error) {

	if (s.State != StateAuthenticated) && (s.State != StateMailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command APPEND cannot be executed in this state", req.Tag),
		}, nil
	}

	// Split payload on every space character.
	createMailboxFolders := strings.Split(req.Payload, " ")

	if len(createMailboxFolders) != 1 {

		// If payload did not contain exactly one element,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command CREATE was not sent with exactly one parameter", req.Tag),
		}, nil
	}

	// Trim supplied mailbox folder name of hierarchy
	// separator if it was sent with a trailing one.
	createMailboxFolder := strings.TrimSuffix(createMailboxFolders[0], mailbox.HierarchySeparator)

	if strings.ToUpper(createMailboxFolder) == "INBOX" {

		// If mailbox folder to-be-created was named INBOX,
		// this is a client error. Return NO response.
		return &Reply{
			Text: fmt.Sprintf("%s NO New mailbox cannot be named INBOX", req.Tag),
		}, nil
	}

	createMaildir := maildir.Dir(filepath.Join(mailbox.MaildirPath, createMailboxFolder))

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	if mailbox.Structure.Lookup(createMailboxFolder) {

		// If mailbox folder to-be-created already exists for user,
		// this is a client error. Return NO response.
		return &Reply{
			Text: fmt.Sprintf("%s NO New mailbox cannot be named after already existing mailbox", req.Tag),
		}, nil
	}

	// Create a new Maildir on stable storage.
	err := createMaildir.Create()
	if err != nil {

		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error while creating Maildir for new mailbox: %v", err)
	}

	// Initialize mail file slice for new mailbox folder
	// to track message sequence numbers in it.
	mailbox.Mails[createMailboxFolder] = make([]string, 0, 6)

	// If succeeded, add the folder as new item to the user's
	// structure CRDT and synchronise with other replicas.
	err = mailbox.Structure.Add(createMailboxFolder, func(args ...string) {
		syncChan <- comm.Msg{
			Operation: "create",
			Create: &comm.Msg_CREATE{
				User:    s.UserName,
				Mailbox: createMailboxFolder,
				AddMailbox: &comm.Msg_Element{
					Value: args[0],
					Tag:   args[1],
				},
			},
		}
	})
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "fail during source CREATE execution, will clean up",
			"err", err,
		)

		// Free space allocated for tracking mail files
		// to be placed into created mailbox folder.
		delete(mailbox.Mails, createMailboxFolder)

		// Attempt to remove Maildir.
		err = createMaildir.Remove()
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove Maildir",
				"err", err,
			)
		}

		os.Exit(1)
	}

	return &Reply{
		Text: fmt.Sprintf("%s OK CREATE completed", req.Tag),
	}, nil
}

// Delete attempts to remove an existing mailbox with
// all included content in CRDT as well as file system.
func (mailbox *Mailbox) Delete(s *Session, req *Request, syncChan chan comm.Msg) (*Reply, error) {

	if (s.State != StateAuthenticated) && (s.State != StateMailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command DELETE cannot be executed in this state", req.Tag),
		}, nil
	}

	// Split payload on every space character.
	deleteMailboxFolders := strings.Split(req.Payload, " ")

	if len(deleteMailboxFolders) != 1 {

		// If payload did not contain exactly one element,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command DELETE was not sent with exactly one parameter", req.Tag),
		}, nil
	}

	// Trim supplied mailbox folder name of hierarchy
	// separator if it was sent with a trailing one.
	deleteMailboxFolder := strings.TrimSuffix(deleteMailboxFolders[0], mailbox.HierarchySeparator)

	if strings.ToUpper(deleteMailboxFolder) == "INBOX" {

		// If mailbox folder to-be-deleted was named INBOX,
		// this is a client error. Return NO response.
		return &Reply{
			Text: fmt.Sprintf("%s NO Forbidden to delete INBOX", req.Tag),
		}, nil
	}

	deleteMaildir := maildir.Dir(filepath.Join(mailbox.MaildirPath, deleteMailboxFolder))

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// TODO: Add routines to take care of mailboxes that
	//       are tagged with a \Noselect tag.

	// Remove element from user's structure CRDT and send out
	// remove update operations to all other replicas.
	err := mailbox.Structure.Remove(deleteMailboxFolder, func(args ...string) {

		// Prepare slice of Element structs to capture all
		// value-tag-pairs to remove in mailbox structure.
		rmvMailboxFolder := make([]*comm.Msg_Element, (len(args) / 2))

		for i := 0; i < (len(args) / 2); i++ {

			rmvMailboxFolder[i] = &comm.Msg_Element{
				Value: args[(2 * i)],
				Tag:   args[((2 * i) + 1)],
			}
		}

		syncChan <- comm.Msg{
			Operation: "delete",
			Delete: &comm.Msg_DELETE{
				User:       s.UserName,
				Mailbox:    deleteMailboxFolder,
				RmvMailbox: rmvMailboxFolder,
				// TODO: ADD ALL MAIL FILES FOR FIELD 'RmvMails'
			},
		}
	})
	if err != nil {

		if err.Error() == "element to be removed not found in set" {

			// Check if error was caused by client, trying to
			// delete an non-existent mailbox.
			return &Reply{
				Text: fmt.Sprintf("%s NO Cannot delete folder that does not exist", req.Tag),
			}, nil
		}

		// Otherwise, this is a write-back error of the updated CRDT
		// file. Reverting actions were already taken, log error.
		level.Error(mailbox.Logger).Log(
			"msg", "failed to remove elements from user's structure CRDT",
			"err", err,
		)

		os.Exit(1)
	}

	delete(mailbox.Mails, deleteMailboxFolder)

	// Remove files associated with the deleted
	// mailbox folder from stable storage.
	err = deleteMaildir.Remove()
	if err != nil {

		// TODO: Maybe think about better way to clean up here?
		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error while deleting Maildir: %v", err)
	}

	return &Reply{
		Text: fmt.Sprintf("%s OK DELETE completed", req.Tag),
	}, nil
}

// List allows clients to learn about the mailboxes
// available and also returns the hierarchy delimiter.
func (mailbox *Mailbox) List(s *Session, req *Request, syncChan chan comm.Msg) (*Reply, error) {

	if (s.State != StateAuthenticated) && (s.State != StateMailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command LIST cannot be executed in this state", req.Tag),
		}, nil
	}

	// Split payload on every space character.
	listArgs := strings.Split(req.Payload, " ")

	if len(listArgs) != 2 {

		// If payload did not contain between exactly two elements,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command LIST was not sent with exactly two arguments", req.Tag),
		}, nil
	}

	if (listArgs[1] != "%") && (listArgs[1] != "*") {

		// If second argument is not one of two wildcards,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command LIST needs either '%%' or '*' as mailbox name", req.Tag),
		}, nil
	}

	mailbox.Lock.RLock()

	// Retrieve all distinct elements present in structure.
	mailboxFolders := mailbox.Structure.GetAllValues()

	// Reserve space for answer.
	listAnswerLines := make([]string, 0, (len(mailboxFolders) - 1))

	for _, mailboxFolder := range mailboxFolders {

		// Split currently considered mailbox folder
		// name at defined hierarchy separator.
		mailboxParts := strings.Split(mailboxFolder, mailbox.HierarchySeparator)

		if (listArgs[1] == "*") || (len(mailboxParts) == 1) {

			// Either always include a mailbox in the response
			// or only when it is a top level mailbox.
			listAnswerLines = append(listAnswerLines, fmt.Sprintf("* LIST () \"%s\" %s", mailbox.HierarchySeparator, mailboxFolder))
		}
	}

	mailbox.Lock.RUnlock()

	var answer string
	for _, listAnswerLine := range listAnswerLines {

		if answer == "" {
			answer = fmt.Sprintf("%s\r\n", listAnswerLine)
		} else {
			answer = fmt.Sprintf("%s%s\r\n", answer, listAnswerLine)
		}
	}

	if answer == "" {
		answer = fmt.Sprintf("%s OK LIST completed", req.Tag)
	} else {
		answer = fmt.Sprintf("%s%s OK LIST completed", answer, req.Tag)
	}

	return &Reply{
		Text: answer,
	}, nil
}

// AppendBegin checks environment conditions and returns
// a message specifying the awaited number of bytes.
func (mailbox *Mailbox) AppendBegin(s *Session, req *Request) (*Await, error) {

	if (s.State != StateAuthenticated) && (s.State != StateMailbox) {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Await{
			Text: fmt.Sprintf("%s BAD Command APPEND cannot be executed in this state", req.Tag),
		}, nil
	}

	// Split payload on every space character.
	// TODO: Fix this for multiple (\Flag1 \Flag2) as parameter.
	appendArgs := strings.Split(req.Payload, " ")
	lenAppendArgs := len(appendArgs)

	if (lenAppendArgs < 2) || (lenAppendArgs > 4) {

		// If payload did not contain between two and four
		// elements, this is a client error.
		// Return BAD statement.
		return &Await{
			Text: fmt.Sprintf("%s BAD Command APPEND was not sent with appropriate number of parameters", req.Tag),
		}, nil
	}

	// Make space for tracking environment characteristics
	// for this command from AppendBegin to AppendEnd.
	appendInProg := &AppendInProg{
		Tag: req.Tag,
	}

	// Depending on amount of arguments, store them.
	var numBytesRaw string
	switch lenAppendArgs {

	case 2:
		appendInProg.Mailbox = appendArgs[0]
		numBytesRaw = appendArgs[1]

	case 3:
		appendInProg.Mailbox = appendArgs[0]
		appendInProg.FlagsRaw = appendArgs[1]
		numBytesRaw = appendArgs[2]

	case 4:
		appendInProg.Mailbox = appendArgs[0]
		appendInProg.FlagsRaw = appendArgs[1]
		appendInProg.DateTimeRaw = appendArgs[2]
		numBytesRaw = appendArgs[3]
	}

	// If user specified INBOX, set it accordingly.
	if strings.ToUpper(appendInProg.Mailbox) == "INBOX" {
		appendInProg.Mailbox = "INBOX"
	}

	// If flags were supplied, parse them.
	if appendInProg.FlagsRaw != "" {

		_, err := ParseFlags(appendInProg.FlagsRaw)
		if err != nil {

			// Parsing flags from APPEND request produced
			// an error. Return tagged BAD response.
			return &Await{
				Text: fmt.Sprintf("%s BAD %s", req.Tag, err.Error()),
			}, nil
		}

		// TODO: Do something with these flags.
	}

	// If date-time was supplied, parse it.
	if appendInProg.DateTimeRaw != "" {
		// TODO: Parse time and do something with it.
	}

	// Parse out how many bytes we are expecting.
	numBytesString := strings.TrimPrefix(numBytesRaw, "{")
	numBytesString = strings.TrimSuffix(numBytesString, "}")

	// Convert string number to int.
	numBytes, err := strconv.Atoi(numBytesString)
	if err != nil {

		// If we were not able to parse out the number,
		// it was probably a client error. Send tagged BAD.
		return &Await{
			Text: fmt.Sprintf("%s BAD Command APPEND did not contain proper literal data byte number", req.Tag),
		}, nil
	}

	if appendInProg.Mailbox == "INBOX" {
		appendInProg.Maildir = maildir.Dir(mailbox.MaildirPath)
	} else {
		appendInProg.Maildir = maildir.Dir(filepath.Join(mailbox.MaildirPath, appendInProg.Mailbox))
	}

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	mailbox.Lock.Lock()

	if mailbox.Structure.Lookup(appendInProg.Mailbox) != true {

		mailbox.Lock.Unlock()

		// If mailbox folder to append message to does not exist,
		// this is a client error. Return NO response.
		return &Await{
			Text: fmt.Sprintf("%s NO [TRYCREATE] Mailbox to append to does not exist", req.Tag),
		}, nil
	}

	// Store created appendInProg context in session.
	s.AppendInProg = appendInProg

	return &Await{
		Text:     "+ Ready for literal data",
		NumBytes: uint32(numBytes),
	}, nil
}

// AppendEnd receives the mail file associated with a
// prior AppendBegin.
func (mailbox *Mailbox) AppendEnd(s *Session, content []byte, syncChan chan comm.Msg) (*Reply, error) {

	defer mailbox.Lock.Unlock()
	defer func() {
		s.AppendInProg = nil
	}()

	// Open a new Maildir delivery.
	appDelivery, err := s.AppendInProg.Maildir.NewDelivery()
	if err != nil {

		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error during delivery creation: %v", err)
	}

	// Write actual message content to file.
	err = appDelivery.Write(content)
	if err != nil {

		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error during writing message during delivery: %v", err)
	}

	// Close and move just created message.
	newKey, err := appDelivery.Close()
	if err != nil {

		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error finishing delivery of new message: %v", err)
	}

	// Follow Maildir's renaming procedure.
	_, err = s.AppendInProg.Maildir.Unseen()
	if err != nil {

		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error executing Unseen() on recently delivered messages: %v", err)
	}

	// Find file name of just delivered mail.
	mailFileNamePath, err := s.AppendInProg.Maildir.Filename(newKey)
	if err != nil {

		return &Reply{
			Text:   "* BAD Internal server error, sorry. Closing connection.",
			Status: 1,
		}, fmt.Errorf("error finding file name of new message: %v", err)
	}
	mailFileName := filepath.Base(mailFileNamePath)

	// Append new mail to mail file tracking slice.
	mailbox.Mails[s.AppendInProg.Mailbox] = append(mailbox.Mails[s.AppendInProg.Mailbox], mailFileName)

	// Add new mail to mailbox' CRDT and send update
	// message to other replicas.
	err = mailbox.Structure.Add(s.AppendInProg.Mailbox, func(args ...string) {
		syncChan <- comm.Msg{
			Operation: "append",
			Append: &comm.Msg_APPEND{
				User:    s.UserName,
				Mailbox: s.AppendInProg.Mailbox,
				AddMail: &comm.Msg_Element{
					Value:    args[0],
					Tag:      args[1],
					Contents: content,
				},
			},
		}
	})
	if err != nil {

		level.Error(mailbox.Logger).Log(
			"msg", "fail during source APPEND execution, will clean up",
			"err", err,
		)

		err := os.Remove(mailFileNamePath)
		if err != nil {
			level.Error(mailbox.Logger).Log(
				"msg", "failed to remove created mail message",
				"err", err,
			)
		}

		os.Exit(1)
	}

	answer := fmt.Sprintf("%s OK APPEND completed", s.AppendInProg.Tag)

	return &Reply{
		Text: answer,
	}, nil
}

// Expunge deletes messages permanently from currently
// selected mailbox that have been flagged as Deleted
// prior to calling this function.
func (mailbox *Mailbox) Expunge(s *Session, req *Request, syncChan chan comm.Msg) (*Reply, error) {

	if s.State != StateMailbox {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD No mailbox selected to expunge", req.Tag),
		}, nil
	}

	if len(req.Payload) > 0 {

		// If payload was not empty to EXPUNGE command,
		// this is a client error. Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command EXPUNGE was sent with extra parameters", req.Tag),
		}, nil
	}

	// Construct path to Maildir to expunge.
	var expMaildir maildir.Dir
	if s.SelectedMailbox == "INBOX" {
		expMaildir = maildir.Dir(filepath.Join(mailbox.MaildirPath, "cur"))
	} else {
		expMaildir = maildir.Dir(filepath.Join(mailbox.MaildirPath, s.SelectedMailbox, "cur"))
	}

	// Reserve space for mails to expunge.
	expMailNums := make([]int, 0, 6)

	// Declare variable to contain answers
	// (e.g. individual remove operations).
	var answer string
	var expAnswerLines []string

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	mailbox.Lock.Lock()
	defer mailbox.Lock.Unlock()

	// Save all mails possibly to delete and
	// number of these files.
	numExpMails := len(mailbox.Mails[s.SelectedMailbox])

	// Only do the work if there are any mails
	// present in mailbox.
	if numExpMails > 0 {

		// Iterate over all mail files in reverse order.
		for i := (numExpMails - 1); i >= 0; i-- {

			// Retrieve all flags of fetched mail.
			mailFlags, err := expMaildir.Flags(mailbox.Mails[s.SelectedMailbox][i], false)
			if err != nil {

				return &Reply{
					Text:   "* BAD Internal server error, sorry. Closing connection.",
					Status: 1,
				}, fmt.Errorf("error retrieving flags for expunging mails: %v", err)
			}

			// Check for presence of \Deleted flag and
			// add to delete set if found.
			if strings.ContainsRune(mailFlags, 'T') {
				expMailNums = append(expMailNums, i)
			}
		}

		// Reserve space for answer lines.
		expAnswerLines = make([]string, 0, len(expMailNums))

		for _, mailSeqNum := range expMailNums {

			// Remove each mail to expunge from mailbox CRDT.
			err := mailbox.MailboxStructure[s.UserName][s.SelectedMailbox].Remove(mailbox.MailboxContents[s.UserName][s.SelectedMailbox][mailSeqNum], func(args ...string) {

				// Prepare slice of Element structs to capture
				// all value-tag-pairs to remove.
				rmvMails := make([]*comm.Msg_Element, (len(args) / 2))

				for i := 0; i < (len(args) / 2); i++ {

					rmvMails[i] = &comm.Msg_Element{
						Value: args[(2 * i)],
						Tag:   args[((2 * i) + 1)],
					}
				}

				syncChan <- comm.Msg{
					Operation: "expunge",
					Expunge: &comm.Msg_EXPUNGE{
						User:    s.UserName,
						Mailbox: s.SelectedMailbox,
						RmvMail: rmvMails,
					},
				}
			})
			if err != nil {

				// This is a write-back error of the updated mailbox CRDT
				// log file. Reverting actions were already taken, log error.
				level.Error(mailbox.Logger).Log(
					"msg", fmt.Sprintf("failed to remove mail '%v' from user's selected mailbox CRDT", mailbox.MailboxContents[s.UserName][s.SelectedMailbox][mailSeqNum]),
					"err", err,
				)
				os.Exit(1)
			}

			// Construct path to file.
			expMailPath := filepath.Join(string(expMaildir), mailbox.MailboxContents[s.UserName][s.SelectedMailbox][mailSeqNum])

			// Remove the file.
			err = os.Remove(expMailPath)
			if err != nil {

				return &Reply{
					Text:   "* BAD Internal server error, sorry. Closing connection.",
					Status: 1,
				}, fmt.Errorf("error while removing expunged mail file from stable storage: %v", err)
			}

			// Immediately remove mail from contents structure.
			realMailSeqNum := mailSeqNum + 1
			mailbox.MailboxContents[s.UserName][s.SelectedMailbox] = append(mailbox.MailboxContents[s.UserName][s.SelectedMailbox][:mailSeqNum], mailbox.MailboxContents[s.UserName][s.SelectedMailbox][realMailSeqNum:]...)

			// Append individual remove answer to answer lines.
			expAnswerLines = append(expAnswerLines, fmt.Sprintf("* %d EXPUNGE", realMailSeqNum))
		}

		for _, expAnswerLine := range expAnswerLines {

			// Append FETCH part with new flags.
			if answer == "" {
				answer = fmt.Sprintf("%s\r\n", expAnswerLine)
			} else {
				answer = fmt.Sprintf("%s%s\r\n", answer, expAnswerLine)
			}
		}
	}

	if answer == "" {
		answer = fmt.Sprintf("%s OK EXPUNGE completed", req.Tag)
	} else {
		answer = fmt.Sprintf("%s%s OK EXPUNGE completed", answer, req.Tag)
	}

	return &Reply{
		Text: answer,
	}, nil
}

// Store takes in message sequence numbers and some set
// of flags to change in those messages and changes the
// attributes for these mails throughout the system.
func (mailbox *Mailbox) Store(s *Session, req *Request, syncChan chan comm.Msg) (*Reply, error) {

	if s.State != StateMailbox {

		// If connection was not in correct state when this
		// command was executed, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD No mailbox selected for store", req.Tag),
		}, nil
	}

	// Split payload on every space character.
	storeArgs := strings.SplitN(req.Payload, " ", 3)

	if len(storeArgs) < 3 {

		// If payload did not contain at least three
		// elements, this is a client error.
		// Return BAD statement.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Command STORE was not sent with three parameters", req.Tag),
		}, nil
	}

	// Parse data item type (second parameter).
	dataItemType := storeArgs[1]

	if (dataItemType != "FLAGS") && (dataItemType != "FLAGS.SILENT") &&
		(dataItemType != "+FLAGS") && (dataItemType != "+FLAGS.SILENT") &&
		(dataItemType != "-FLAGS") && (dataItemType != "-FLAGS.SILENT") {

		// If supplied data item type is none of the
		// supported ones, this is a client error.
		// Send tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD Unknown data item type specified", req.Tag),
		}, nil
	}

	// If client requested not to receive the updated
	// flags list, set indicator to false.
	silent := false
	if strings.HasSuffix(dataItemType, ".SILENT") {
		silent = true
	}

	// Parse flag argument (third parameter).
	flags, err := ParseFlags(storeArgs[2])
	if err != nil {

		// Parsing flags from STORE request produced
		// an error. Return tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD %s", req.Tag, err.Error()),
		}, nil
	}

	// Set currently selected mailbox with respect to special
	// case of INBOX as current location.
	var selectedMailbox string
	if s.SelectedMailbox == "INBOX" {
		selectedMailbox = mailbox.MaildirPath
	} else {
		selectedMailbox = filepath.Join(mailbox.MaildirPath, s.SelectedMailbox)
	}

	storeMaildir := maildir.Dir(selectedMailbox)

	// Lock node exclusively to make execution
	// of following CRDT operations atomic.
	mailbox.Lock.Lock()

	numMails := len(mailbox.Mails[s.SelectedMailbox])

	// Parse sequence numbers argument (first parameter).
	// CAUTION: We expect this function to fail if supplied
	//          message sequence numbers did not refer to
	//          existing messages in mailbox.
	mailSeqNums, err := ParseSeqNumbers(storeArgs[0], numMails)
	if err != nil {

		mailbox.Lock.Unlock()

		// Parsing sequence numbers from STORE request produced
		// an error. Return tagged BAD response.
		return &Reply{
			Text: fmt.Sprintf("%s BAD %s", req.Tag, err.Error()),
		}, nil
	}

	// Prepare answer slice.
	answerLines := make([]string, 0, len(mailSeqNums))

	for _, mailSeqNum := range mailSeqNums {

		// Initialize runes slice for new flags of mail.
		newMailFlags := make([]rune, 0, 5)

		// Depending on the presence of various standard
		// flags extend newMailFlags further.

		if _, found := flags["\\Draft"]; found {
			newMailFlags = append(newMailFlags, 'D')
		}

		if _, found := flags["\\Flagged"]; found {
			newMailFlags = append(newMailFlags, 'F')
		}

		if _, found := flags["\\Answered"]; found {
			newMailFlags = append(newMailFlags, 'R')
		}

		if _, found := flags["\\Seen"]; found {
			newMailFlags = append(newMailFlags, 'S')
		}

		if _, found := flags["\\Deleted"]; found {
			newMailFlags = append(newMailFlags, 'T')
		}

		mailFileName := mailbox.Mails[s.SelectedMailbox][mailSeqNum]

		// Read message content from file.
		mailFileContent, err := ioutil.ReadFile(filepath.Join(selectedMailbox, "cur", mailFileName))
		if err != nil {

			mailbox.Lock.Unlock()

			return &Reply{
				Text:   "* BAD Internal server error, sorry. Closing connection.",
				Status: 1,
			}, fmt.Errorf("error while reading in mail file content in STORE operation: %v", err)
		}

		// Retrieve flags included in mail file name.
		mailFlags, err := storeMaildir.Flags(mailFileName, false)
		if err != nil {

			mailbox.Lock.Unlock()

			return &Reply{
				Text:   "* BAD Internal server error, sorry. Closing connection.",
				Status: 1,
			}, fmt.Errorf("error while retrieving flags from mail file: %v", err)
		}

		// Check if supplied flags should be added
		// to existing flags if not yet present.
		if (dataItemType == "+FLAGS") || (dataItemType == "+FLAGS.SILENT") {

			newMailFlagsString := string(newMailFlags)

			for _, char := range mailFlags {

				// Iterate over all characters of the currently present
				// flags of the mail and check if they are present in
				// new flags string as well. If not, add it.
				if strings.ContainsRune(newMailFlagsString, char) != true {
					newMailFlags = append(newMailFlags, char)
				}
			}
		}

		// Check if supplied flags should be removed
		// from existing flags if they are present.
		if (dataItemType == "-FLAGS") || (dataItemType == "-FLAGS.SILENT") {

			tmpNewMailFlags := mailFlags

			for _, char := range newMailFlags {

				// Iterate over all characters of the supplied new
				// flags and check if they are present in current
				// flags string. If so, remove them from intermediate
				// variable that holds former flags.
				if strings.ContainsRune(mailFlags, char) {
					tmpNewMailFlags = strings.Replace(tmpNewMailFlags, string(char), "", -1)
				}
			}

			// Reset new flags slice to reduced intermediate value.
			newMailFlags = []rune(tmpNewMailFlags)
		}

		// Check if we really have to perform an update
		// across the system or if we can save the energy.
		if mailFlags != string(newMailFlags) {

			// Set just constructed new flags string in mail's
			// file name (renaming it).
			newMailFileName, err := storeMaildir.SetFlags(mailFileName, string(newMailFlags), false)
			if err != nil {

				mailbox.Lock.Unlock()

				return &Reply{
					Text:   "* BAD Internal server error, sorry. Closing connection.",
					Status: 1,
				}, fmt.Errorf("error renaming mail file in STORE operation: %v", err)
			}

			var rmvMails []*comm.Msg_Element

			// First, remove the former name of the mail file
			// but do not yet send out an update operation.
			err = mailbox.MailboxStructure[s.UserName][s.SelectedMailbox].Remove(mailFileName, func(args ...string) {

				// Prepare slice of Element structs to capture
				// all value-tag-pairs to remove.
				rmvMails = make([]*comm.Msg_Element, (len(args) / 2))

				for i := 0; i < (len(args) / 2); i++ {

					rmvMails[i] = &comm.Msg_Element{
						Value: args[(2 * i)],
						Tag:   args[((2 * i) + 1)],
					}
				}
			})
			if err != nil {

				// This is a write-back error of the updated mailbox CRDT
				// log file. Reverting actions were already taken, log error.
				level.Error(mailbox.Logger).Log(
					"msg", "failed to remove old mail name from selected mailbox CRDT",
					"err", err,
				)
				mailbox.Lock.Unlock()
				os.Exit(1)
			}

			// Second, add the new mail file's name and finally
			// instruct all other nodes to do the same.
			err = mailbox.MailboxStructure[s.UserName][s.SelectedMailbox].Add(newMailFileName, func(args ...string) {

				syncChan <- comm.Msg{
					Operation: "store",
					Store: &comm.Msg_STORE{
						User:    s.UserName,
						Mailbox: s.SelectedMailbox,
						RmvMail: rmvMails,
						AddMail: &comm.Msg_Element{
							Value:    args[0],
							Tag:      args[1],
							Contents: mailFileContent,
						},
					},
				}
			})
			if err != nil {

				// This is a write-back error of the updated mailbox CRDT
				// log file. Reverting actions were already taken, log error.
				level.Error(mailbox.Logger).Log(
					"msg", "failed to add renamed mail name to selected mailbox CRDT",
					"err", err,
				)
				mailbox.Lock.Unlock()
				os.Exit(1)
			}

			// If we are done with that, also replace the mail's
			// file name in the corresponding mail file slice.
			mailbox.Mails[s.SelectedMailbox][mailSeqNum] = newMailFileName
		}

		if silent != true {

			answerFlagString := ""

			for _, newFlag := range newMailFlags {

				switch newFlag {

				case 'D':

					if answerFlagString == "" {
						answerFlagString = "\\Draft"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Draft", answerFlagString)
					}

				case 'F':

					if answerFlagString == "" {
						answerFlagString = "\\Flagged"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Flagged", answerFlagString)
					}

				case 'R':

					if answerFlagString == "" {
						answerFlagString = "\\Answered"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Answered", answerFlagString)
					}

				case 'S':

					if answerFlagString == "" {
						answerFlagString = "\\Seen"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Seen", answerFlagString)
					}

				case 'T':

					if answerFlagString == "" {
						answerFlagString = "\\Deleted"
					} else {
						answerFlagString = fmt.Sprintf("%s \\Deleted", answerFlagString)
					}

				}

			}

			// Append this file's FETCH answer.
			realMailSeqNum := mailSeqNum + 1
			answerLines = append(answerLines, fmt.Sprintf("* %d FETCH (FLAGS (%s))", realMailSeqNum, answerFlagString))
		}
	}

	mailbox.Lock.Unlock()

	var answer string
	if silent != true {

		for _, answerLine := range answerLines {

			// Append FETCH part with new flags.
			if answer == "" {
				answer = fmt.Sprintf("%s\r\n", answerLine)
			} else {
				answer = fmt.Sprintf("%s%s\r\n", answer, answerLine)
			}
		}
	}

	if answer == "" {
		answer = fmt.Sprintf("%s OK STORE completed", req.Tag)
	} else {
		answer = fmt.Sprintf("%s%s OK STORE completed", answer, req.Tag)
	}

	return &Reply{
		Text: answer,
	}, nil
}
