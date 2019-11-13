package flexmy_test

import (
	"errors"
	"testing"
	"time"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/toolkit"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	connString = "mysql://root:Database.1@/testdb"
	tableName  = "testmodel"
)

type dataObject struct {
	ID        string
	Title     string
	DataDec   float64
	DataGroup string
	Created   time.Time
}

func newDataObject(id string, group string) *dataObject {
	do := new(dataObject)
	do.ID = id
	do.Title = "Title for " + id
	do.DataGroup = group
	do.DataDec = float64(toolkit.RandFloat(1000, 2))
	do.Created = time.Now()
	return do
}

func connect() (dbflex.IConnection, error) {
	dbflex.Logger().SetLevelStdOut(toolkit.ErrorLevel, true)
	dbflex.Logger().SetLevelStdOut(toolkit.InfoLevel, true)
	dbflex.Logger().SetLevelStdOut(toolkit.WarningLevel, true)
	dbflex.Logger().SetLevelStdOut(toolkit.DebugLevel, true)

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

func TestQueryDropCreateTable(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("drop table", func() {
			conn.DropTable(tableName)
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("ensure table", func() {
				err = conn.EnsureTable(tableName, []string{"ID"}, newDataObject("", ""))
				cv.So(err, cv.ShouldBeNil)
			})
		})
	})
}

func TestQueryM(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("saving data", func() {
			cmd := dbflex.From(tableName).Where(dbflex.Eq("id", "e1")).Save()
			_, err := conn.Execute(cmd, toolkit.M{}.Set("data", &dataObject{"E1", "Emp01", 20.37, "", time.Now()}))
			cv.So(err, cv.ShouldBeNil)

			cv.Convey("querying", func() {
				cmd := dbflex.From(tableName).Select()
				cur := conn.Cursor(cmd, nil)
				defer cur.Close()
				cv.So(cur.Error(), cv.ShouldBeNil)

				cv.Convey("get results", func() {
					ms := []toolkit.M{}
					err := cur.Fetchs(&ms, 0)
					cv.So(err, cv.ShouldBeNil)
					cv.So(len(ms), cv.ShouldBeGreaterThan, 0)
				})
			})
		})

	})
}

func TestQueryObj(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("querying", func() {
			cmd := dbflex.From(tableName).Select()
			cur := conn.Cursor(cmd, nil)
			defer cur.Close()
			cv.So(cur.Error(), cv.ShouldBeNil)

			cv.Convey("get results", func() {
				ms := []dataObject{}
				err := cur.Fetchs(&ms, 2)
				cv.So(err, cv.ShouldBeNil)
				cv.So(len(ms), cv.ShouldBeGreaterThan, 0)

				cv.Printf("\nResults:\n%s\n", toolkit.JsonString(ms))
			})
		})
	})
}

func TestQueryDelete(t *testing.T) {
	cv.Convey("connecting", t, func() {
		conn, err := connect()
		cv.So(err, cv.ShouldBeNil)
		defer conn.Close()

		cv.Convey("insert data 100x ", func() {
			for i := 0; i < 100; i++ {
				cmd := dbflex.From(tableName).Insert()
				_, err := conn.Execute(cmd, toolkit.M{}.Set("data", newDataObject(toolkit.RandomString(10), "QD")))
				if err != nil {
					cv.Println("error saving.", err.Error())
				}
			}
			cursor := conn.Cursor(dbflex.From(tableName).Where(dbflex.Eq("datagroup", "QD")).Select(), nil)
			cv.So(cursor.Error(), cv.ShouldEqual, nil)
			count := cursor.Count()
			cv.So(count, cv.ShouldBeGreaterThan, 99)

			cv.Convey("delete fews data", func() {
				dos := make([]dataObject, 5)
				cursor.Fetchs(&dos, 5)
				cursor.Close()

				for _, do := range dos {
					//fmt.Println("deleting", do.ID)
					cmdDel := dbflex.From(tableName).Delete().Where(dbflex.Eq("id", do.ID))
					_, err := conn.Execute(cmdDel, nil)
					if err != nil {
						cv.Println("unable to delete", do.ID, " error:", err.Error())
					}
				}

				conn1, _ := connect()
				defer conn1.Close()
				cursor1 := conn1.Cursor(dbflex.From(tableName).Select().Where(dbflex.Eq("datagroup", "QD")), nil)
				defer cursor1.Close()
				countAfterDel := cursor1.Count()
				cv.So(countAfterDel, cv.ShouldEqual, count-5)
			})
		})
	})
}
