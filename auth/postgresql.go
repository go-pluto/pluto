package auth

import (
	"fmt"

	"github.com/jinzhu/gorm"

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

// GetIDOfUser finds position of supplied user in users
// table. It is assumed that existence check was already
// performed, for example via AuthenticatePlain.
func (p *PostgreSQLAuthenticator) GetOriginalIDOfUser(username string) int {

	return -1
}

// GetTokenOfUser returns the currently assigned token as
// a sign of a valid authentication for a supplied name.
func (p *PostgreSQLAuthenticator) GetTokenOfUser(username string) string {

	return ""
}

// DeleteTokenOfUser deletes the currently assigned token,
// logging the user out of the system.
func (p *PostgreSQLAuthenticator) DeleteTokenOfUser(id int) {}

// AuthenticatePlain performs the actual authentication
// process by taking supplied credentials and attempting
// to find a matching entry in PostgreSQL database described
// by a struct of above's layout.
func (p *PostgreSQLAuthenticator) AuthenticatePlain(username string, password string) (*int, *string, error) {

	return nil, nil, nil
}
