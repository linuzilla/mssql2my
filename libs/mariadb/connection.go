package mariadb

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"

	"github.com/linuzilla/mssql2my/libs/dbconn"
)

type mariadbConnection struct {
	dbuser          string
	dbname          string
	dsn             string
	driver          string
	server          string
	option          string
	logmode         bool
	integerProperty int
	stringProperty  string
}

func (db *mariadbConnection) testConnection() {
	conn, err := sql.Open(db.driver, db.dsn)

	if err != nil {
		fmt.Println("Failed to Connect to database")
		log.Fatal(err)
	}

	defer func() {
		conn.Close()
	}()

	fmt.Printf("Test Mariadb database Connectivity [ %s / %s ]... ", db.server, db.dbname)

	if stmt, err := conn.Prepare("SELECT USER()"); err != nil {
		fmt.Println(err)
	} else {
		if rows, err := stmt.Query(); err != nil {
			fmt.Println("Failed to query")
		} else {
			for rows.Next() {
				var user string
				if err := rows.Scan(&user); err != nil {
					fmt.Println(err)
				} else {
					if strings.HasPrefix(user, db.dbuser) {
						fmt.Println("good")
					} else {
						fmt.Println("not sure!")
					}
					rows.Close()
				}
			}
		}
	}

}

func (svc *mariadbConnection) GetConnection(callback func(xdb *dbconn.DB) (interface{}, error)) (interface{}, error) {
	if conn, err := gorm.Open(svc.driver, svc.dsn); err != nil {
		return nil, err
	} else {
		defer conn.Close()
		conn.LogMode(svc.logmode)
		return callback(&dbconn.DB{Gorm: conn, SqlDB: conn.DB()})
	}
}

func (svc *mariadbConnection) GetExtraProperty(property string) interface{} {
	return svc.integerProperty
}

func (svc *mariadbConnection) GetDialect() dbconn.IDbDialect {
	return dialect
}

func New(dbinfo *dbconn.DBConnectionInfo) dbconn.IDBConnection {
	data, err := base64.StdEncoding.DecodeString(dbinfo.Passwd)
	if err != nil {
		fmt.Println("Password must encoded in Base64")
		log.Fatal(err)
	}

	passwd := string(data)
	self := &mariadbConnection{
		dbuser:          dbinfo.User,
		dbname:          dbinfo.Database,
		driver:          dbinfo.Driver,
		server:          dbinfo.Server,
		option:          dbinfo.Option,
		logmode:         dbinfo.LogMode,
		integerProperty: dbinfo.IntegerTag,
		stringProperty:  dbinfo.StringTag,
	}

	options := `charset=utf8mb4&parseTime=true&loc=Local`

	if self.option != "" {
		options = self.option
	}

	if dbinfo.Server[0] == '/' {
		self.dsn = dbinfo.User + ":" + passwd + "@unix(" + dbinfo.Server + ")/" + dbinfo.Database + "?" + options
	} else {
		self.dsn = dbinfo.User + ":" + passwd + "@tcp(" + dbinfo.Server + ")/" + dbinfo.Database + "?" + options
	}

	if dbinfo.TestOnBoot {
		self.testConnection()
	}
	return self
}
