package dbutils

import (
	"bytes"
	"fmt"
	"github.com/linuzilla/mssql2my/libs/dbconn"
	"log"
	"reflect"
)

type dbTable struct {
	name       string
	db         *dbDatabase
	columns    []string
	colMap     map[string]*dbColumn
	primaryKey []string
	indexes    []*dbconn.DbIndex
}

func (self *dbTable) Name() string {
	return self.name
}

func (self *dbTable) Database() dbconn.IDatabase {
	return self.db
}

func (self *dbTable) Columns() []string {
	return self.columns
}

func (self *dbTable) Column(colName string) dbconn.IDbColumn {
	if entry, found := self.colMap[colName]; found {
		return entry
	}
	return nil
}

func (self *dbTable) PrimaryKey() []string {
	return self.primaryKey
}

func (self *dbTable) Indexes() []*dbconn.DbIndex {
	return self.indexes
}

func (self *dbTable) DropTableQuery() string {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`", self.name)
}

func (self *dbTable) ShowCreateTable() string {
	var buffer bytes.Buffer

	fmt.Fprintf(&buffer, "CREATE TABLE IF NOT EXISTS `%s` (\n", self.name)
	for i, colname := range self.columns {
		if i != 0 {
			fmt.Fprintln(&buffer, ",")
		}
		col := self.colMap[colname]

		if col.col.OctetLength.Valid && col.col.OctetLength.Int64 > 1000 && col.col.MariadbType == `varchar` {
			fmt.Fprintf(&buffer, "  `%s` text", colname)
		} else {

			if col.col.OctetLength.Valid {
				if col.col.OctetLength.Int64 >= 0 {
					fmt.Fprintf(&buffer, "  `%s` %s", colname, col.col.MariadbType)
					fmt.Fprintf(&buffer, "(%d)", col.col.OctetLength.Int64)
				} else if col.col.MariadbType == `varchar` {
					fmt.Fprintf(&buffer, "  `%s` %s", colname, `text`)
				} else {
					fmt.Fprintf(&buffer, "  `%s` %s", colname, col.col.MariadbType)
				}
			} else {
				fmt.Fprintf(&buffer, "  `%s` %s", colname, col.col.MariadbType)
			}
		}

		if col.col.Nullable {
			fmt.Fprint(&buffer, " DEFAULT NULL")
		} else {
			fmt.Fprint(&buffer, " NOT NULL")
			if col.col.ColumnDefault.Valid {
				fmt.Fprintf(&buffer, " DEFAULT '%s'", col.col.ColumnDefault.String)
			}
		}
	}
	if self.primaryKey != nil && len(self.primaryKey) != 0 {
		fmt.Fprint(&buffer, ",\n  PRIMARY KEY (")
		for i, k := range self.primaryKey {
			if i != 0 {
				fmt.Fprint(&buffer, ",")
			}
			fmt.Fprintf(&buffer, "`%s`", k)
		}
		fmt.Fprint(&buffer, ")")
	}

	if self.indexes != nil && len(self.indexes) != 0 {
		for _, index := range self.indexes {
			fmt.Fprint(&buffer, ",\n  ")
			if index.Unique {
				fmt.Fprint(&buffer, "UNIQUE ")
			}
			if len(index.Name) > 30 {
				fmt.Fprintf(&buffer, "KEY `%s` (", index.Name[0:30])
			} else {
				fmt.Fprintf(&buffer, "KEY `%s` (", index.Name)
			}
			for i, col := range index.Columns {
				if i != 0 {
					fmt.Fprint(&buffer, ",")
				}
				fmt.Fprintf(&buffer, "`%s`", col)
			}
			fmt.Fprint(&buffer, ")")
		}
	}

	fmt.Fprint(&buffer, "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	return buffer.String()
}

func (self *dbTable) generateQuery() string {
	var buffer bytes.Buffer

	dialect := self.db.impl.dbc.GetDialect()

	fmt.Fprint(&buffer, "SELECT ")

	for i, colname := range self.columns {
		if i != 0 {
			fmt.Fprint(&buffer, ",")
		}
		fmt.Fprintf(&buffer, "%s%s%s", dialect.Bra(), colname, dialect.Ket())
	}

	fmt.Fprintf(&buffer, " FROM %s%s%s", dialect.Bra(), self.name, dialect.Ket())

	return buffer.String()
}

func (self *dbTable) SelectAll(callback func(columns []string, args ...interface{}) bool) error {
	if _, err := self.db.impl.dbc.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
		db := xdb.SqlDB

		query := self.generateQuery()
		fmt.Println(query)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		colnum := len(self.columns)

		valuePtrs := make([]interface{}, colnum, colnum)
		values := make([]interface{}, colnum, colnum)

		for rows.Next() {
			for i, colname := range self.columns {
				col := self.colMap[colname]
				values[i] = reflect.New(col.col.ColumnType).Elem().Interface()
				valuePtrs[i] = &values[i]
			}
			rows.Scan(valuePtrs...)
			if !callback(self.columns, values...) {
				break
			}
		}
		return nil, nil
	}); err != nil {
		return err
	} else {
		return nil
	}
}
