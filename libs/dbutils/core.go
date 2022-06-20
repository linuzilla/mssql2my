package dbutils

import (
	"errors"

	"github.com/linuzilla/mssql2my/libs/dbconn"
	"github.com/linuzilla/mssql2my/libs/mariadb"
	"github.com/linuzilla/mssql2my/libs/mssql"
)

const (
	DriveNotSupport = `driver not support!`
)

type dbutilImpl struct {
	dbi       *dbconn.DBConnectionInfo
	dbc       dbconn.IDBConnection
	dbx       dbconn.IDBConnectionExtended
	databases map[string]*dbDatabase
}

func (self *dbutilImpl) GetDatabase() (dbconn.IDatabase, error) {
	return self.GetDatabaseByName(self.dbi.Database)
}

func (self *dbutilImpl) GetDatabaseByName(dbName string) (dbconn.IDatabase, error) {
	if entry, found := self.databases[dbName]; found {
		return entry, nil
	}

	if self.dbx == nil {
		return nil, errors.New(DriveNotSupport)
	}

	tables, err := self.dbx.GetTables(dbName)
	if err != nil {
		return nil, err
	}

	result := &dbDatabase{
		impl:     self,
		name:     dbName,
		tables:   tables,
		tableMap: make(map[string]*dbTable),
	}

	return result, nil
}

func chooseDatabase(db *dbconn.DBConnectionInfo) dbconn.IDBConnection {
	if db == nil {
		return nil
	} else {
		switch db.Driver {
		case "mariadb", "mysql":
			return mariadb.New(db)
		case "mssql":
			return mssql.New(db)
		}
		return nil
	}
}

func New(dbinfo *dbconn.DBConnectionInfo) dbconn.IDatabaseUtil {
	self := &dbutilImpl{
		dbi: dbinfo,
		dbc: chooseDatabase(dbinfo),
	}

	if db, ok := self.dbc.(dbconn.IDBConnectionExtended); ok {
		self.dbx = db
	}
	return self
}

func (self *dbutilImpl) GetDBConnection() dbconn.IDBConnection {
	return self.dbc
}

func (self *dbutilImpl) getTable(db *dbDatabase, tableName string) (*dbTable, error) {
	if self.dbx == nil {
		return nil, errors.New(DriveNotSupport)
	}

	columns, err := self.dbx.GetColumns(db.name, tableName)
	if err != nil {
		return nil, err
	}
	cols := make([]string, len(columns), len(columns))
	colMap := make(map[string]*dbColumn)

	pk, err := self.dbx.GetPrimaryKey(db.name, tableName)
	if err != nil {
		return nil, err
	}

	idx, err := self.dbx.GetIndexes(db.name, tableName)
	if err != nil {
		return nil, err
	}

	result := &dbTable{
		name:       tableName,
		db:         db,
		columns:    cols,
		colMap:     colMap,
		primaryKey: pk,
		indexes:    idx,
	}

	for i, entry := range columns {
		cols[i] = entry.Name
		colMap[entry.Name] = &dbColumn{
			name:  entry.Name,
			table: result,
			col:   entry,
		}
	}

	return result, nil
}
