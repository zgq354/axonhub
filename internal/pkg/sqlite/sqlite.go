package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"modernc.org/sqlite"
)

// https://github.com/ent/ent/discussions/1667
type sqliteDriver struct {
	*sqlite.Driver
}

func (d sqliteDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return conn, err
	}

	//nolint:forcetypeassert
	c := conn.(interface {
		Exec(stmt string, args []driver.Value) (driver.Result, error)
	})
	if _, err := c.Exec("PRAGMA foreign_keys = on;", nil); err != nil {
		if err := conn.Close(); err != nil {
			return nil, fmt.Errorf("failed to close connection: %w", err)
		}

		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	return conn, nil
}

// NewDriver returns the driver that is registered as "sqlite3". Other packages
// wrap it (see the transaction instrumentation in internal/server/db), so the
// instance has to be obtainable rather than only registered by name.
func NewDriver() driver.Driver {
	return sqliteDriver{Driver: &sqlite.Driver{}}
}

func init() {
	sql.Register("sqlite3", NewDriver())
}
