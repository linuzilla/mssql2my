package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/linuzilla/mssql2my/libs/dbconn"
	"github.com/linuzilla/mssql2my/libs/dbutils"

	"gopkg.in/yaml.v2"
)

type SubConfig struct {
	Database dbconn.DBConnectionInfo `yaml:"database"`
	Tables   []string                `yaml:"tables"`
}

type Config struct {
	SourceDB      map[string]SubConfig    `yaml:"source"`
	DestinationDB dbconn.DBConnectionInfo `yaml:"destination"`
}

func main() {
	config_file := `config.yaml`

	content, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Fatal(err)
	}

	config := &Config{}
	err = yaml.Unmarshal(content, config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	dbDest := dbutils.New(&config.DestinationDB)

	for _, entry := range config.SourceDB {
		Clone(dbutils.New(&entry.Database), dbDest, entry.Tables)
	}

	record(dbDest)
}

func record(dbDest dbconn.IDatabaseUtil) {
	queries := [...]string{
		"CREATE TABLE IF NOT EXISTS `__db_sync` (`id` int(11) NOT NULL AUTO_INCREMENT, `sync_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,   PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1",
		"INSERT INTO __db_sync (`sync_at`) VALUES (NOW())",
	}

	dbDestConnection := dbDest.GetDBConnection()

	for _, query := range queries {
		fmt.Println(query)
		dbDestConnection.GetConnection(func(xdb *dbconn.DB) (interface{}, error) {
			xdb.SqlDB.Exec(query)
			return nil, nil
		})
	}
}

func Clone(dbSrc, dbDest dbconn.IDatabaseUtil, tableToClone []string) {
	dbSrcDB, err := dbSrc.GetDatabase()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	tables := make(map[string]bool)

	for _, t := range dbSrcDB.Tables() {
		tables[t] = false
	}

	doThis := true
	continueNext := true

	for _, t := range tableToClone {
		if _, found := tables[t]; found {
			if tbl := dbSrcDB.Table(t); tbl != nil {
				fmt.Println("Clone table:", tbl.Name())
				records, wrote, err := dbDest.CloneTable(tbl, func(columns []string, variables []interface{}) (bool, bool) {
					return doThis, continueNext
				})
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("Table [%s]: red %d record(s), wrote %d record(s)\n",
					tbl.Name(), records, wrote)

			}
		} else {
			log.Fatal(t, ": not found")
		}
	}
}
