package dbconn

import (
	"database/sql"
	"github.com/jinzhu/gorm"
)

type DB struct {
	Gorm  *gorm.DB
	SqlDB *sql.DB
}

type IDBConnection interface {
	GetConnection(callback func(xdb *DB) (interface{}, error)) (interface{}, error)
	GetExtraProperty(property string) interface{}
	GetDialect() IDbDialect
}

type IMigratable interface {
	AutoMigrate(xdb *DB)
}

type IHaveForeignKey interface {
	AddForeignKey(xdb *DB)
}

type IDBConnectionExtended interface {
	GetTables(dbName string) ([]string, error)
	GetColumns(dbName, tableName string) ([]DbColumn, error)
	GetPrimaryKey(dbName, dbTable string) ([]string, error)
	GetIndexes(dbName, dbTable string) ([]*DbIndex, error)
}
