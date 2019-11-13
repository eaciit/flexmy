package flexmy

import (
	"database/sql"
	"fmt"
	"reflect"
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

// DropTable - delete table
func (c *Connection) DropTable(name string) error {
	_, err := c.db.Exec("drop table if exists " + name)
	return err
}

// EnsureTable ensure existence and structures of the table
func (c *Connection) EnsureTable(name string, keys []string, obj interface{}) error {
	cmd := fmt.Sprintf("select table_name from information_schema.TABLES t where table_type='BASE TABLE' and table_name='%s'", name)
	rs, err := c.db.Query(cmd)
	if err != nil {
		return fmt.Errorf("unable to check table existence. %s", err.Error())
	}
	defer rs.Close()

	tableExists := false
	for rs.Next() {
		tbname := ""
		rs.Scan(&tbname)
		tableExists = tbname == name
		break
	}

	v := reflect.Indirect(reflect.ValueOf(obj))
	t := v.Type()

	if !tableExists {
		//-- create table
		cmd = "CREATE TABLE %s (\n%s\n)"

		fieldNum := t.NumField()
		fields := make([]string, fieldNum)
		idx := 0
		for idx < fieldNum {
			ft := t.Field(idx)
			dataType := "VARCHAR(200)"
			ftName := strings.ToLower(ft.Type.Name())
			if strings.HasPrefix(ftName, "int") {
				dataType = "INT"
			} else if strings.HasPrefix(ftName, "float") {
				dataType = "REAL"
			} else if strings.HasPrefix(ftName, "time") {
				dataType = "DATETIME"
			}
			ftxt := fmt.Sprintf("%s %s", ft.Name, dataType)
			if toolkit.HasMember(keys, ft.Name) {
				ftxt = ftxt + " NOT NULL PRIMARY KEY"
			}
			fields[idx] = ftxt
			idx++
		}
		cmd = fmt.Sprintf(cmd, name, strings.Join(fields, ",\n"))
		//fmt.Println("command:\n", cmd)
		_, err = c.db.Exec(cmd)
		if err != nil {
			return fmt.Errorf("unable to created table %s. %s", name, err.Error())
		}
	} else {
		//fmt.Println("table", name, "is exist")
		return fmt.Errorf("table %s already exist. Please do manual DDL change", name)
	}
	return nil
}
