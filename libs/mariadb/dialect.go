package mariadb

type mariadbDialect int

var dialect mariadbDialect

func (mariadbDialect) Bra() string {
	return "`"
}

func (mariadbDialect) Ket() string {
	return "`"
}
