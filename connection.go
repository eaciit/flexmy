package flexmy

import (
	"database/sql"
	"strings"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"

	"git.eaciitapp.com/sebar/dbflex/drivers/rdbms"
	_ "github.com/go-sql-driver/mysql"
)

// Connection implementation of dbflex.IConnection
type Connection struct {
	rdbms.Connection
	db *sql.DB
}

func init() {
	dbflex.RegisterDriver("mysql", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.SetThis(c)
		c.ServerInfo = *si
		return c
	})
}

// Connect to database instance
func (c *Connection) Connect() error {
	sqlconnstring := toolkit.Sprintf("tcp(%s)/%s", c.Host, c.Database)
	if c.User != "" {
		sqlconnstring = toolkit.Sprintf("%s:%s@%s", c.User, c.Password, sqlconnstring)
	}
	configs := strings.Join(func() []string {
		var out []string
		for k, v := range c.Config {
			out = append(out, toolkit.Sprintf("%s=%s", k, v))
		}
		return out
	}(), "&")
	if configs != "" {
		sqlconnstring = sqlconnstring + "?" + configs
	}
	db, err := sql.Open("mysql", sqlconnstring)
	c.db = db
	return err
}

func (c *Connection) State() string {
	if c.db != nil {
		return dbflex.StateConnected
	}
	return dbflex.StateUnknown
}

// Close database connection
func (c *Connection) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

// NewQuery generates new query object to perform query action
func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.db = c.db
	return q
}
