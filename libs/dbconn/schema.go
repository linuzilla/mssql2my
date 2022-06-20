package dbconn

import (
	"database/sql"
	"reflect"
)

type IDatabaseUtil interface {
	GetDBConnection() IDBConnection
	GetDatabase() (IDatabase, error)
	GetDatabaseByName(dbname string) (IDatabase, error)
	CloneTable(tbl IDbTable, intercept func(columns []string, variables []interface{}) (bool, bool)) (int, int, error)
}

type IDatabase interface {
	Name() string
	Tables() []string
	Table(tableName string) IDbTable
}

type IDbTable interface {
	Name() string
	Database() IDatabase
	Columns() []string
	PrimaryKey() []string
	Column(colName string) IDbColumn
	Indexes() []*DbIndex
	ShowCreateTable() string
	DropTableQuery() string
	SelectAll(callback func(columns []string, args ...interface{}) bool) error
}

type IDbColumn interface {
	Name() string
	Table() IDbTable
	Database() IDatabase
}

type DbColumn struct {
	Name          string
	TypeName      string
	MariadbType   string
	Nullable      bool
	ColumnDefault sql.NullString
	OctetLength   sql.NullInt64
	ColumnType    reflect.Type
}

type DbIndex struct {
	Name    string
	Columns []string
	Unique  bool
}
