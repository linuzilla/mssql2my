package dbutils

import (
	"fmt"

	"github.com/linuzilla/mssql2my/libs/dbconn"
)

type dbColumn struct {
	col   dbconn.DbColumn
	name  string
	table *dbTable
}

func (self *dbColumn) Name() string {
	return self.name
}

func (self *dbColumn) Table() dbconn.IDbTable {
	return self.table
}

func (self *dbColumn) Database() dbconn.IDatabase {
	return self.table.Database()
}

func (self *dbColumn) String() string {
	return fmt.Sprintf("%s %s(%d) nullable=%v",
		self.name, self.col.TypeName, self.col.OctetLength.Int64, self.col.Nullable)
}
