package flexmy_test

import (
	"errors"
	"testing"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	connString = "mysql://root:Database.1@/ectestdb"
	tableName  = "testmodel"
)

func connect() (dbflex.IConnection, error) {
	conn, err := dbflex.NewConnectionFromURI(connString, nil)
	if err != nil {
		return nil, errors.New("unable to connect. " + err.Error())
	}
	err = conn.Connect()
	if err != nil {
		return nil, errors.New("unable to connect. " + err.Error())
	}
	return conn, nil
}

func TestQueryM(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("querying", func() {
			cmd := dbflex.From(tableName).Select()
			cur := conn.Cursor(cmd, nil)
			cv.So(cur.Error(), cv.ShouldBeNil)

			cv.Convey("get results", func() {
				ms := []toolkit.M{}
				err := cur.Fetchs(&ms, 0)
				cv.So(err, cv.ShouldBeNil)

				toolkit.Printfn("\nResults:\n%s\n", toolkit.JsonString(ms))
			})
		})
	})
}
