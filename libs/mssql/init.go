package mssql

import (
	"database/sql"
	"reflect"
)

type typeMapStruct struct {
	reftype  reflect.Type
	nreftype reflect.Type
	maptype  string
}

var typeMap map[string]typeMapStruct
var strType, intType, floatType, boolType reflect.Type
var nstrType, nintType, nfloatType, nboolType reflect.Type

func init() {
	strType = reflect.TypeOf(``)
	intType = reflect.TypeOf(1)
	floatType = reflect.TypeOf(0.1)
	boolType = reflect.TypeOf(true)
	nstrType = reflect.TypeOf(sql.NullString{})
	nintType = reflect.TypeOf(sql.NullInt64{})
	nfloatType = reflect.TypeOf(sql.NullFloat64{})
	nboolType = reflect.TypeOf(sql.NullBool{})

	typeMap = map[string]typeMapStruct{
		`int`: typeMapStruct{
			reftype:  intType,
			nreftype: nintType,
		},
		`smallint`: typeMapStruct{
			reftype:  intType,
			nreftype: nintType,
		},
		`tinyint`: typeMapStruct{
			reftype:  intType,
			nreftype: nintType,
		},
		`bigint`: typeMapStruct{
			reftype:  intType,
			nreftype: nintType,
		},
		`char`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
		},
		`nchar`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `varchar`,
		},
		`varchar`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
		},
		`nvarchar`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `varchar`,
		},
		`text`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
		},
		`ntext`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `text`,
		},
		`xml`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `text`,
		},
		`decimal`: typeMapStruct{
			reftype:  floatType,
			nreftype: nfloatType,
		},
		`float`: typeMapStruct{
			reftype:  floatType,
			nreftype: nfloatType,
		},
		`bit`: typeMapStruct{
			reftype:  boolType,
			nreftype: nboolType,
			maptype:  `tinyint`,
		},
		`varbinary`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `blob`,
		},
		`binary`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `blob`,
		},
		`image`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `blob`,
		},
		`timestamp`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
		},
		`datetime`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
		},
		`datetime2`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `datetime`,
		},
		`smalldatetime`: typeMapStruct{
			reftype:  strType,
			nreftype: nstrType,
			maptype:  `datetime`,
		},
	}
}
