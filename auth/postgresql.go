package auth

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/numbleroot/pluto/config"

	// We need fitting PostgreSQL drivers for gorm.
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

// Structs

// PostgreSQLAuthenticator stores connection information
// to PostgreSQL database table configured in system.
type PostgreSQLAuthenticator struct {
	IP         string
	Port       string
	Database   string
	User       string
	Connection *gorm.DB
}

// Functions

// NewPostgreSQLAuthenticator handles the initialization
// of the database connection and returns all information
// nicely packaged in above struct.
func NewPostgreSQLAuthenticator(ip string, port string, db string, user string, pass string, sslmode string) (*PostgreSQLAuthenticator, error) {

	var conn *gorm.DB
	var err error

	// Either attempt login with or without password to database.
	if pass != "" {
		conn, err = gorm.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, ip, port, db, sslmode))
	} else {
		conn, err = gorm.Open("postgres", fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=%s", user, ip, port, db, sslmode))
	}
	if err != nil {
		return nil, fmt.Errorf("[auth.NewPostgreSQLAuthenticator] Could not connect to database: %s\n", err.Error())
	}

	// Try to reach database.
	err = conn.DB().Ping()
	if err != nil {
		return nil, fmt.Errorf("[auth.NewPostgreSQLAuthenticator] Specified database not reachable after connection: %s\n", err.Error())
	}

	return &PostgreSQLAuthenticator{
		IP:         ip,
		Port:       port,
		Database:   db,
		User:       user,
		Connection: conn,
	}, nil
}

// GetOriginalIDOfUser finds position of supplied user in users
// table. It is assumed that existence check was already performed,
// for example via AuthenticatePlain.
func (p *PostgreSQLAuthenticator) GetOriginalIDOfUser(username string) int {

	return -1
}

// GetWorkerForUser returns the name of the worker node
// that is responsible for handling the user's mailbox.
func (p *PostgreSQLAuthenticator) GetWorkerForUser(workers map[string]config.Worker, id int) (string, error) {

	return "", fmt.Errorf("no worker responsible for user ID %d", id)
}

// DeleteTokenForUser removes the token associated with
// an active session of a client.
func (p *PostgreSQLAuthenticator) DeleteTokenForUser(username string) {}

// AuthenticatePlain performs the actual authentication
// process by taking supplied credentials and attempting
// to find a matching entry in PostgreSQL database described
// by a struct of above's layout.
func (p *PostgreSQLAuthenticator) AuthenticatePlain(username string, password string) (int, string, error) {

	return -1, "", nil
}
