package flexmy

import (
	"git.eaciitapp.com/sebar/dbflex/drivers/rdbms"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}
