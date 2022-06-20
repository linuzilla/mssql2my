package mssql

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	"github.com/linuzilla/mssql2my/libs/dbconn"
	"log"
	"reflect"
	"strings"
)

type msSqlConnection struct {
	dbuser          string
	dsn             string
	driver          string
	dbserver        string
	option          string
	dbname          string
	logmode         bool
	integerProperty int
	stringProperty  string
}

func (svc *msSqlConnection) testConnection() {
	fmt.Printf("MSSQL [ %s ] [ %s ] database Connectivity ... ", svc.dbserver, svc.dbname)

	conn, err := sql.Open(svc.driver, svc.dsn)

	if err != nil {
		fmt.Println("Failed to Connect to database")
		log.Fatal(err)
	}

	defer conn.Close()

	if stmt, err := conn.Prepare("SELECT CURRENT_USER,DB_NAME()"); err != nil {
		fmt.Println("Failed to prepare")
	} else {
		if rows, err := stmt.Query(); err != nil {
			fmt.Println("Failed to query")
		} else {
			for rows.Next() {
				var user string
				var dbname string
				if err := rows.Scan(&user, &dbname); err != nil {
					fmt.Println("oops!")
				} else {
					if strings.HasPrefix(user, svc.dbuser) {
						fmt.Println("good")
					} else {
						fmt.Println("not sure!")
					}
				}
			}
			rows.Close()
		}
		stmt.Close()
	}
}

func (svc *msSqlConnection) GetConnection(callback func(xdb *dbconn.DB) (interface{}, error)) (interface{}, error) {
	if conn, err := gorm.Open(svc.driver, svc.dsn); err != nil {
		return nil, err
	} else {
		defer conn.Close()
		conn.LogMode(svc.logmode)
		return callback(&dbconn.DB{Gorm: conn, SqlDB: conn.DB()})
	}
}

func (svc *msSqlConnection) GetDialect() dbconn.IDbDialect {
	return dialect
}

func (svc *msSqlConnection) GetTables(dbName string) ([]string, error) {
	if result, err := svc.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
		return svc.RetrieveTables(xdb, dbName)
	}); err != nil {
		return nil, err
	} else {
		return result.([]string), nil
	}
}

func (svc *msSqlConnection) GetColumns(dbName, dbTable string) ([]dbconn.DbColumn, error) {
	if result, err := svc.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
		return svc.RetrieveColumns(xdb, dbName, dbTable)
	}); err != nil {
		return nil, err
	} else {
		return result.([]dbconn.DbColumn), nil
	}
}

func (svc *msSqlConnection) GetPrimaryKey(dbName, dbTable string) ([]string, error) {
	if result, err := svc.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
		return svc.RetrievePrimaryKey(xdb, dbName, dbTable)
	}); err != nil {
		return nil, err
	} else {
		return result.([]string), nil
	}
}

func (svc *msSqlConnection) GetIndexes(dbName, dbTable string) ([]*dbconn.DbIndex, error) {
	if result, err := svc.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
		return svc.RetrieveIndexes(xdb, dbName, dbTable)
	}); err != nil {
		return nil, err
	} else {
		return result.([]*dbconn.DbIndex), nil
	}
}

func (svc *msSqlConnection) RetrieveTables(xdb *dbconn.DB, dbname string) ([]string, error) {
	db := xdb.SqlDB
	rows, err := db.Query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG=?", dbname)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []string{}

	for rows.Next() {
		var tableName string

		if err := rows.Scan(&tableName); err != nil {
			log.Println(err)
			break
		} else {
			list = append(list, tableName)
		}
	}
	return list, nil
}

func (svc *msSqlConnection) RetrieveIndexes(xdb *dbconn.DB, dbName, tableName string) ([]*dbconn.DbIndex, error) {
	db := xdb.SqlDB
	/* TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME	COLUMN_NAME	CONSTRAINT_CATALOG	CONSTRAINT_SCHEMA	CONSTRAINT_NAME */

	rows, err := db.Query(
		`SELECT i.name AS IndexName, 
		        o.name AS TableName, 
			ic.key_ordinal AS ColumnOrder,
			i.is_unique AS IsUnique,
			co.[name] AS ColumnName
			FROM sys.indexes i 
			JOIN sys.objects o ON i.object_id = o.object_id
			JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
			JOIN sys.columns co on co.object_id = i.object_id AND co.column_id = ic.column_id
			WHERE o.name = ? AND i.is_primary_key = 0
			ORDER by o.[name], i.[name], ic.is_included_column, ic.key_ordinal`,
		tableName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	list := []*dbconn.DbIndex{}
	var currentIndex *dbconn.DbIndex

	for rows.Next() {
		var indexName, tbName, colName string
		var colOrder int
		var isUnique bool

		if err := rows.Scan(&indexName, &tbName, &colOrder, &isUnique, &colName); err != nil {
			log.Fatal(err)
			break
		} else {
			if currentIndex == nil || currentIndex.Name != indexName {
				currentIndex = &dbconn.DbIndex{
					Name:    indexName,
					Columns: []string{},
					Unique:  isUnique,
				}
				list = append(list, currentIndex)
			}
			currentIndex.Columns = append(currentIndex.Columns, colName)
		}
	}
	return list, nil
}

func (svc *msSqlConnection) RetrievePrimaryKey(xdb *dbconn.DB, dbName, tableName string) ([]string, error) {
	db := xdb.SqlDB
	/* TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME	COLUMN_NAME	CONSTRAINT_CATALOG	CONSTRAINT_SCHEMA	CONSTRAINT_NAME */

	rows, err := db.Query(
		`SELECT Col.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col WHERE Col.Constraint_Name = Tab.Constraint_Name AND Col.Table_Name = Tab.Table_Name AND Constraint_Type = 'PRIMARY KEY' AND Col.TABLE_NAME = ? AND Col.TABLE_CATALOG = ?`,
		tableName, dbName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	list := []string{}
	for rows.Next() {
		var columnName string

		if err := rows.Scan(&columnName); err != nil {
			log.Fatal(err)
			break
		} else {
			list = append(list, columnName)
		}
	}
	return list, nil
}

func convertType(dataType string, nullable bool) (reflect.Type, string) {
	if entry, found := typeMap[dataType]; found {
		mtypeName := entry.maptype
		if mtypeName == `` {
			mtypeName = dataType
		}
		if nullable {
			return entry.nreftype, mtypeName
		} else {
			return entry.reftype, mtypeName
		}
	} else {
		if nullable {
			return nstrType, "`" + dataType + "`"
		} else {
			return strType, "`" + dataType + "`"
		}
	}
}

func (svc *msSqlConnection) RetrieveColumns(xdb *dbconn.DB, dbName, tableName string) ([]dbconn.DbColumn, error) {
	db := xdb.SqlDB

	/*
		TABLE_CATALOG
		TABLE_SCHEMA
		TABLE_NAME
		COLUMN_NAME
		ORDINAL_POSITION
		COLUMN_DEFAULT
		IS_NULLABLE
		DATA_TYPE
		CHARACTER_MAXIMUM_LENGTH
		CHARACTER_OCTET_LENGTH
		NUMERIC_PRECISION
		NUMERIC_PRECISION_RADIX
		NUMERIC_SCALE
		DATETIME_PRECISION
		CHARACTER_SET_CATALOG
		CHARACTER_SET_SCHEMA
		CHARACTER_SET_NAME
		COLLATION_CATALOG
		COLLATION_SCHEMA
		COLLATION_NAME
		DOMAIN_CATALOG
		DOMAIN_SCHEMA
		DOMAIN_NAME
	*/

	rows, err := db.Query("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_OCTET_LENGTH  FROM information_schema.COLUMNS WHERE (TABLE_NAME = ? AND TABLE_CATALOG = ?) ORDER BY ORDINAL_POSITION",
		tableName, dbName)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer rows.Close()

	list := []dbconn.DbColumn{}
	for rows.Next() {
		var columnName string
		var dataType string
		var nullable sql.NullString
		var columnDefault sql.NullString
		var octetLength sql.NullInt64

		if err := rows.Scan(&columnName, &dataType, &nullable, &columnDefault, &octetLength); err != nil {
			log.Fatal(err)
			break
		} else {
			isNullable := nullable.Valid && nullable.String == "YES"

			columnType, mariadbType := convertType(dataType, isNullable)

			list = append(list, dbconn.DbColumn{
				Name:          columnName,
				TypeName:      dataType,
				Nullable:      isNullable,
				ColumnDefault: columnDefault,
				OctetLength:   octetLength,
				ColumnType:    columnType,
				MariadbType:   mariadbType,
			})
		}
	}
	return list, nil
}

func (svc *msSqlConnection) GetExtraProperty(property string) interface{} {
	return svc.integerProperty
}

func New(dbinfo *dbconn.DBConnectionInfo) dbconn.IDBConnection {
	data, err := base64.StdEncoding.DecodeString(dbinfo.Passwd)
	if err != nil {
		fmt.Println("Password must encoded in Base64")
		log.Fatal(err)
	}

	passwd := string(data)

	dsn := fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;port=1433",
		dbinfo.Server,
		dbinfo.Database,
		dbinfo.User,
		passwd,
	)
	self := &msSqlConnection{
		dbuser:          dbinfo.User,
		dbname:          dbinfo.Database,
		driver:          `mssql`,
		dsn:             dsn,
		dbserver:        dbinfo.Server,
		option:          dbinfo.Option,
		logmode:         dbinfo.LogMode,
		integerProperty: dbinfo.IntegerTag,
		stringProperty:  dbinfo.StringTag,
	}

	if dbinfo.TestOnBoot {
		self.testConnection()
	}
	return self
}
