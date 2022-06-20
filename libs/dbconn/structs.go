package dbconn

type DBConnectionInfo struct {
	Server     string `toml:"server" yaml:"server"`
	User       string `toml:"user" yaml:"user"`
	Passwd     string `toml:"password" yaml:"password"`
	Database   string `toml:"database" yaml:"database"`
	Driver     string `toml:"driver" yaml:"driver"`
	Option     string `toml:"option" yaml:"option"`
	LogMode    bool   `toml:"logmode" yaml:"logmode"`
	TestOnBoot bool   `toml:"test-on-boot" yaml:"test-on-boot"`
	Disable    bool   `toml:"disable" yaml:"disable"`
	IntegerTag int    `toml:"integer-tag" yaml:"integer-tag"`
	StringTag  string `toml:"string-tag" yaml:"string-tag"`
}
