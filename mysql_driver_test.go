package flexmy_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eaciit/toolkit"
	_ "github.com/go-sql-driver/mysql"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	connTxt = "root:Database.1@/ectestdb"
)

func clasicConnect() (*sql.DB, error) {
	return sql.Open("mysql", connTxt)
}

func TestClassicMysql(t *testing.T) {
	cv.Convey("connect", t, func() {
		db, err := clasicConnect()
		cv.So(err, cv.ShouldBeNil)
		defer db.Close()

		cv.Convey("querying data", func() {
			cmd := "select * from testmodel"
			rows, err := db.Query(cmd)
			cv.So(err, cv.ShouldBeNil)
			defer rows.Close()

			cv.Convey("get the metadata", func() {
				columnNames, errColumnName := rows.Columns()
				columnTypes, errColumnType := rows.ColumnTypes()

				cv.So(errColumnName, cv.ShouldBeNil)
				cv.So(len(columnNames), cv.ShouldBeGreaterThan, 0)
				cv.So(errColumnType, cv.ShouldBeNil)
				cv.So(len(columnTypes), cv.ShouldBeGreaterThan, 0)

				sqlTypes := []string{}
				values := [][]byte{}
				valuePtrs := []interface{}{}
				for _, ct := range columnTypes {
					name := strings.ToLower(ct.DatabaseTypeName())
					//fmt.Println(columnNames[idx], " |", name)
					if strings.HasPrefix(name, "int") {
						sqlTypes = append(sqlTypes, "int")
						//values = append(values, int(0))
					} else if strings.HasPrefix(name, "dec") || strings.HasPrefix(name, "float") {
						sqlTypes = append(sqlTypes, "float64")
						//values = append(values, float64(0))
					} else if strings.HasPrefix(name, "datetime") {
						sqlTypes = append(sqlTypes, "time.Time")
						//values = append(values, time.Time{})
					} else {
						sqlTypes = append(sqlTypes, "string")
						//values = append(values, "")
					}
					values = append(values, []byte{})
				}

				for idx, _ := range values {
					valuePtrs = append(valuePtrs, &values[idx])
				}

				fmt.Println("\ncolumns: ", toolkit.JsonString(columnNames),
					"\ntypes:", toolkit.JsonString(sqlTypes))

				cv.Convey("validating data", func() {
					for {
						if rows.Next() {
							scanErr := rows.Scan(valuePtrs...)
							if scanErr != nil {
								cv.So(scanErr, cv.ShouldBeNil)
								break
							}
							fmt.Println("values:", toolkit.JsonString(values))
							m := toolkit.M{}
							for idx, v := range values {
								name := columnNames[idx]
								ft := sqlTypes[idx]
								switch ft {
								case "int":
									m.Set(name, toolkit.ToInt(string(v), toolkit.RoundingAuto))

								case "float64":
									m.Set(name, toolkit.ToFloat64(string(v), 4, toolkit.RoundingAuto))

								case "time.Time":
									if dt, err := time.Parse(time.RFC3339, string(v)); err == nil {
										m.Set(name, dt)
									} else {
										dt = toolkit.String2Date(string(v), "yyyy-MM-dd hh:mm:ss")
										m.Set(name, dt)
									}

								default:
									m.Set(name, string(v))
								}
							}
							toolkit.Println("data:", toolkit.JsonString(m))
						} else {
							break
						}
					}
				})
			})
		})
	})
}
