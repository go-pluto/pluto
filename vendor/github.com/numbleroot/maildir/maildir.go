// The maildir package provides an interface to mailboxes in the Maildir format.
package maildir

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"crypto/rand"
	"encoding/hex"
	"net/mail"
	"net/textproto"
	"path/filepath"
	"sync/atomic"
)

// Constants

// CreateMode holds the permissions used when creating a directory.
const CreateMode = 0700

// Variables

// The separator separates a message's unique key from its flags in the filename.
// This should only be changed on operating systems where the colon isn't
// allowed in filenames.
var separator rune = ':'

// id will be used to created a unique new mail file name (key).
var id int64 = 10000

// Structs and Types

// A Dir represents a single directory in a Maildir mailbox.
type Dir string

type runeSlice []rune

// A KeyError occurs when a key matches more or less than one message.
type KeyError struct {
	Key string // the (invalid) key
	N   int    // number of matches (!= 1)
}

// A FlagError occurs when a non-standard info section is encountered.
type FlagError struct {
	Info         string // the encountered info section
	Experimental bool   // info section starts with 1
}

// Delivery represents an ongoing message delivery to the mailbox. It
// implements the WriteCloser interface. When associated Close function
// is called, the underlying file is moved from tmp to new directory.
type Delivery struct {
	file *os.File
	d    Dir
	key  string
}

// Functions

func (e *KeyError) Error() string {
	return "maildir: key " + e.Key + " matches " + strconv.Itoa(e.N) + " files."
}

func (e *FlagError) Error() string {

	if e.Experimental {
		return "maildir: experimental info section encountered: " + e.Info[2:]
	}

	return "maildir: bad info section encountered: " + e.Info
}

func (s runeSlice) Len() int           { return len(s) }
func (s runeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s runeSlice) Less(i, j int) bool { return s[i] < s[j] }

// Unseen moves messages from new to cur and returns their keys.
// This means the messages are now known to the application.
// To find out whether a user has seen a message, use Flags().
func (d Dir) Unseen() ([]string, error) {

	f, err := os.Open(filepath.Join(string(d), "new"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	names, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	var keys []string

	for _, n := range names {

		if n[0] != '.' {

			split := strings.FieldsFunc(n, func(r rune) bool {
				return (r == separator)
			})

			key := split[0]
			info := "2,"

			// Messages in new shouldn't have an info section but
			// we act as if, in case some other program didn't
			// follow the spec.
			if len(split) > 1 {
				info = split[1]
			}

			// Save key for later return.
			keys = append(keys, key)

			// Build up new file name for mail in cur directory.
			curFileName := (key + string(separator) + info)

			// Rename file from old name in new directory to
			// curFileName in new directory.
			err = os.Rename(filepath.Join(string(d), "new", n), filepath.Join(string(d), "cur", curFileName))
			if err != nil {
				return nil, err
			}
		}
	}

	return keys, nil
}

// Keys returns a list of file names to use as keys in order
// to access mails in the cur directory of a Maildir.
func (d Dir) Keys() ([]string, error) {

	f, err := os.Open(filepath.Join(string(d), "cur"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	names, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	var keys []string

	for _, n := range names {

		if n[0] != '.' {

			split := strings.FieldsFunc(n, func(r rune) bool {
				return (r == separator)
			})

			keys = append(keys, split[0])
		}
	}

	return keys, nil
}

// Filename returns the path to the file corresponding to the key.
func (d Dir) Filename(key string) (string, error) {

	// Find matching files in cur directory that begin with key.
	matches, err := filepath.Glob(filepath.Join(string(d), "cur", (key + "*")))
	if err != nil {
		return "", err
	}

	if n := len(matches); n != 1 {

		// If there was more than one match, return an error.
		return "", &KeyError{
			Key: key,
			N:   n,
		}
	}

	// Otherwise, return the match.
	return matches[0], nil
}

// Header returns the corresponding mail header to a key.
func (d Dir) Header(key string) (*mail.Header, error) {

	// Check if there exists only one file matching the key.
	filename, err := d.Filename(key)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// In order to parse out the leading MIME header of
	// a mail file, create a new textproto convenience reader.
	tp := textproto.NewReader(bufio.NewReader(file))

	// Extract the MIME header.
	hdr, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, err
	}

	// Cast MIME header to net/mail's header type.
	header := mail.Header(hdr)

	return &header, nil
}

// Message returns a message by key.
func (d Dir) Message(key string) (*mail.Message, error) {

	filename, err := d.Filename(key)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := new(bytes.Buffer)

	_, err = io.Copy(buf, file)
	if err != nil {
		return nil, err
	}

	// Parse headers and make available the mail body.
	msg, err := mail.ReadMessage(buf)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Flags returns the flags for a message sorted in ascending order.
// See the documentation of SetFlags for details.
func (d Dir) Flags(key string, isKey bool) (string, error) {

	var err error
	var filename string

	if isKey {

		filename, err = d.Filename(key)
		if err != nil {
			return "", err
		}
	} else {
		filename = key
	}

	split := strings.FieldsFunc(filename, func(r rune) bool {
		return (r == separator)
	})

	switch {

	case len(split[1]) < 2, split[1][1] != ',':

		// Info section of mail file name did not
		// start with a version identifier.
		return "", &FlagError{
			Info:         split[1],
			Experimental: false,
		}

	case split[1][0] == '1':

		// Experimental version of Maildir specified
		// which we will not process.
		return "", &FlagError{
			Info:         split[1],
			Experimental: true,
		}

	case split[1][0] != '2':

		// Any other Maildir version specified which
		// is not 2. Will not process.
		return "", &FlagError{
			Info:         split[1],
			Experimental: false,
		}
	}

	rs := runeSlice(split[1][2:])
	sort.Sort(rs)

	return string(rs), nil
}

// SetFlags appends an info section to the filename according to the given flags.
// This function removes duplicates and sorts the flags, but doesn't check
// whether they conform with the Maildir specification.
//
// The following flags are listed in the specification
// (http://cr.yp.to/proto/maildir.html):
//
//	Flag "D" (draft): the user considers this message a draft; toggled at user discretion.
//	Flag "F" (flagged): user-defined flag; toggled at user discretion.
//	Flag "P" (passed): the user has resent/forwarded/bounced this message to someone else.
//	Flag "R" (replied): the user has replied to this message.
//	Flag "S" (seen): the user has viewed this message, though perhaps he didn't read all the way through it.
//	Flag "T" (trashed): the user has moved this message to the trash; the trash will be emptied by a later user action.
func (d Dir) SetFlags(key string, flags string, isKey bool) (string, error) {

	// Maildir version number 2.
	info := "2,"

	rs := runeSlice(flags)
	sort.Sort(rs)

	for _, r := range rs {

		if []rune(info)[len(info)-1] != r {
			info += string(r)
		}
	}

	// Write info to mail file name.
	newFileName, err := d.SetInfo(key, info, isKey)
	if err != nil {
		return "", err
	}

	return newFileName, nil
}

// Set info part of file name. Only use this if
// you plan on using a non-standard info part.
func (d Dir) SetInfo(key, info string, isKey bool) (string, error) {

	var err error
	var filename string

	if isKey {

		// If key was supplied, find corresponding
		// file name for key.
		filename, err = d.Filename(key)
		if err != nil {
			return "", err
		}
	} else {

		// If filename was supplied, split filename
		// at separator symbol.
		split := strings.FieldsFunc(key, func(r rune) bool {
			return (r == separator)
		})

		// Set filename to key and set correct key.
		filename = filepath.Join(string(d), "cur", key)
		key = split[0]
	}

	// Build up new name of mail file.
	newFileName := key + string(separator) + info

	// Rename mail to new name.
	err = os.Rename(filename, filepath.Join(string(d), "cur", newFileName))
	if err != nil {
		return "", err
	}

	return newFileName, nil
}

// Key generates a new unique key as described in the Maildir specification.
// For the third part of the key (delivery identifier) it uses an internal
// counter, the process id and a cryptographical random number to ensure
// uniqueness among messages delivered in the same second.
func Key() (string, error) {

	// Start with empty string.
	key := ""

	// First part of key is current time in seconds
	// since 1970 (UNIX timestamp).
	key += strconv.FormatInt(time.Now().Unix(), 10)
	key += "."

	host, err := os.Hostname()
	if err != err {
		return "", err
	}

	// Second part is the host name with some
	// escaped characters.
	host = strings.Replace(host, "/", "\057", -1)
	host = strings.Replace(host, string(separator), "\072", -1)
	key += host
	key += "."

	// Third part is made up of three elements:
	// * first, the process ID
	key += strconv.FormatInt(int64(os.Getpid()), 10)

	// * second, an atomically incremented counter value.
	key += strconv.FormatInt(id, 10)
	atomic.AddInt64(&id, 1)

	// * third, 10 bytes of random characters from a
	//   cryptographically secure pseudo-random number generator.
	bs := make([]byte, 10)
	_, err = io.ReadFull(rand.Reader, bs)
	if err != nil {
		return "", err
	}
	key += hex.EncodeToString(bs)

	return key, nil
}

// Check takes in supplied directory and performs
// compliance checks to Maildir folder layout.
// It does not change anything in file system in
// case it encounters an error.
func (d Dir) Check() error {

	correctPerm := fmt.Sprintf("%v", (os.ModeDir | CreateMode))

	dir, err := os.Stat(string(d))
	if err != nil {
		return err
	}

	if dir.Mode().String() != correctPerm {
		return fmt.Errorf("selected folder is not a directory or has wrong permissions")
	}

	tmpDir, err := os.Stat(filepath.Join(string(d), "tmp"))
	if err != nil {
		return err
	}

	if tmpDir.Mode().String() != correctPerm {
		return fmt.Errorf("tmp directory in selected folder is not a directory or has wrong permissions")
	}

	newDir, err := os.Stat(filepath.Join(string(d), "new"))
	if err != nil {
		return err
	}

	if newDir.Mode().String() != correctPerm {
		return fmt.Errorf("new directory in selected folder is not a directory or has wrong permissions")
	}

	curDir, err := os.Stat(filepath.Join(string(d), "cur"))
	if err != nil {
		return err
	}

	if curDir.Mode().String() != correctPerm {
		return fmt.Errorf("cur directory in selected folder is not a directory or has wrong permissions")
	}

	return nil
}

// Create creates the directory structure for a Maildir.
// If the main directory already exists, it tries to create the subdirectories
// in there. If an error occurs while creating one of the subdirectories, this
// function may leave a partially created directory structure.
func (d Dir) Create() error {

	err := os.Mkdir(string(d), (os.ModeDir | CreateMode))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	err = os.Mkdir(filepath.Join(string(d), "tmp"), (os.ModeDir | CreateMode))
	if err != nil {
		return err
	}

	err = os.Mkdir(filepath.Join(string(d), "new"), (os.ModeDir | CreateMode))
	if err != nil {
		return err
	}

	err = os.Mkdir(filepath.Join(string(d), "cur"), (os.ModeDir | CreateMode))
	if err != nil {
		return err
	}

	return nil
}

// Remove deletes an entire Maildir from stable storage.
func (d Dir) Remove() error {

	// Attempt to remove Maildir with all children.
	err := os.RemoveAll(string(d))
	if err != nil {
		return err
	}

	return nil
}

// NewDelivery creates a new Delivery.
func (d Dir) NewDelivery() (*Delivery, error) {

	// Generate a new mail file key.
	key, err := Key()
	if err != nil {
		return nil, err
	}

	// Set a timed function for new delivery that
	// aborts the delivery attempt after 24 hours.
	del := new(Delivery)
	time.AfterFunc((24 * time.Hour), func() {
		del.Abort()
	})

	// Create the new mail file under built key
	// in tmp directory of Maildir.
	file, err := os.Create(filepath.Join(string(d), "tmp", key))
	if err != nil {
		return nil, err
	}

	del.file = file
	del.d = d
	del.key = key

	// Sync file creation to disk before continuing.
	err = del.file.Sync()
	if err != nil {
		return nil, err
	}

	return del, nil
}

// Write takes in supplied slice of bytes representing
// the actual mail contents to be written into mail
// file in the delivery process.
func (d *Delivery) Write(p []byte) error {

	// Write bytes to file.
	_, err := d.file.Write(p)
	if err != nil {
		return err
	}

	// Sync changes to stable storage
	// (flush to hard disk).
	err = d.file.Sync()
	if err != nil {
		return err
	}

	return nil
}

// Close closes the underlying file and moves it to new.
func (d *Delivery) Close() (string, error) {

	err := d.file.Close()
	if err != nil {
		return "", err
	}

	// Hard-link delivered message from tmp to new directory.
	err = os.Link(filepath.Join(string(d.d), "tmp", d.key), filepath.Join(string(d.d), "new", d.key))
	if err != nil {
		return "", err
	}

	// Remove the old reference in tmp folder.
	err = os.Remove(filepath.Join(string(d.d), "tmp", d.key))
	if err != nil {
		return "", err
	}

	return d.key, nil
}

// Abort closes the underlying file and removes it completely.
func (d *Delivery) Abort() error {

	err := d.file.Close()
	if err != nil {
		return err
	}

	// If timeout expired, remove file in tmp folder.
	err = os.Remove(filepath.Join(string(d.d), "tmp", d.key))
	if err != nil {
		return err
	}

	return nil
}

// Move moves a message from this Maildir to another.
func (d Dir) Move(target Dir, key string) error {

	path, err := d.Filename(key)
	if err != nil {
		return err
	}

	// Rename link to file from source directory
	// to cur folder at target directory.
	err = os.Rename(path, filepath.Join(string(target), "cur", filepath.Base(path)))
	if err != nil {
		return err
	}

	return nil
}

// Purge removes the actual file behind this message.
func (d Dir) Purge(key string) error {

	f, err := d.Filename(key)
	if err != nil {
		return err
	}

	// Unlink file and thereby delete it.
	err = os.Remove(f)
	if err != nil {
		return err
	}

	return nil
}

// Clean removes old files from tmp and should be run periodically.
// This does not use access time but modification time for portability reasons.
func (d Dir) Clean() error {

	f, err := os.Open(filepath.Join(string(d), "tmp"))
	if err != nil {
		return err
	}
	defer f.Close()

	names, err := f.Readdirnames(0)
	if err != nil {
		return err
	}

	// Save current time for comparison later on.
	now := time.Now()

	for _, name := range names {

		// Gather information on file by stat'ing it.
		file, err := os.Stat(filepath.Join(string(d), "tmp", name))
		if err != nil {
			// Go back to for loop beginning in case of an error.
			continue
		}

		// Calculate if last modification time of file in
		// tmp directory is more than 36 hours ago.
		if now.Sub(file.ModTime()).Hours() > 36 {

			// If this is the case, we can safely remove it.
			err = os.Remove(filepath.Join(string(d), "tmp", name))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
