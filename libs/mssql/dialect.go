package mssql

type mssqlDialect int

var dialect mssqlDialect

func (mssqlDialect) Bra() string {
	return "["
}

func (mssqlDialect) Ket() string {
	return "]"
}
