package dbutils

import (
	"bytes"
	"fmt"
	"github.com/linuzilla/mssql2my/libs/dbconn"
	"log"
)

func (self *dbutilImpl) dropTable(xdb *dbconn.DB, tbl dbconn.IDbTable) error {
	db := xdb.SqlDB
	query := tbl.DropTableQuery()
	fmt.Println(query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (self *dbutilImpl) createTable(xdb *dbconn.DB, tbl dbconn.IDbTable) error {
	db := xdb.SqlDB
	query := tbl.ShowCreateTable()
	fmt.Println(query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (dbutilImpl) generateInsertStatement(dialect dbconn.IDbDialect, tbl dbconn.IDbTable) string {
	var buffer bytes.Buffer

	fmt.Fprintf(&buffer, "INSERT INTO %s%s%s (",
		dialect.Bra(),
		tbl.Name(),
		dialect.Ket(),
	)
	for i, colname := range tbl.Columns() {
		if i != 0 {
			fmt.Fprint(&buffer, ",")
		}
		fmt.Fprintf(&buffer, "%s%s%s", dialect.Bra(), colname, dialect.Ket())
	}
	fmt.Fprint(&buffer, ") VALUES (")

	for i, _ := range tbl.Columns() {
		if i != 0 {
			fmt.Fprint(&buffer, ",")
		}
		fmt.Fprint(&buffer, "?")
	}
	fmt.Fprint(&buffer, ")")

	return buffer.String()
}

func (self *dbutilImpl) CloneTable(tbl dbconn.IDbTable, intercept func(columns []string, variables []interface{}) (bool, bool)) (int, int, error) {
	records, wrote := 0, 0

	_, err := self.dbc.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
		self.dropTable(xdb, tbl)
		self.createTable(xdb, tbl)
		query := self.generateInsertStatement(self.dbc.GetDialect(), tbl)
		fmt.Println(query)

		stmt, err := xdb.SqlDB.Prepare(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer stmt.Close()

		tbl.SelectAll(func(columns []string, variables ...interface{}) bool {
			doit, conti := true, true
			records++

			if intercept != nil {
				doit, conti = intercept(columns, variables)
			}
			if doit {
				_, err := stmt.Exec(variables...)
				if err != nil {
					log.Println(variables)
					log.Println(err)
					// return false
				}
				wrote++
			}
			return conti
		})
		return nil, nil
	})

	return records, wrote, err
}
