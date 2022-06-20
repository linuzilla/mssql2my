package dbconn

type IDbDialect interface {
	Bra() string
	Ket() string
}
