package dbutils

import (
	"github.com/linuzilla/mssql2my/libs/dbconn"
	"log"
)

// Database

type dbDatabase struct {
	impl     *dbutilImpl
	name     string
	tables   []string
	tableMap map[string]*dbTable
}

func (self *dbDatabase) Name() string {
	return self.name
}

func (self *dbDatabase) Tables() []string {
	return self.tables
}

func (self *dbDatabase) Table(tableName string) dbconn.IDbTable {
	if entry, found := self.tableMap[tableName]; found {
		return entry
	}
	results, err := self.impl.getTable(self, tableName)
	if err != nil {
		log.Println(err)
		return nil
	}
	self.tableMap[tableName] = results
	return results
}
