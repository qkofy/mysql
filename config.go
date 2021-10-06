package mysql

type Config struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
	Charset  string
	Prefix   string
	Conn     bool
	Debug    bool
	Explain  bool
}

func (cfg *Config) Configure() *Config {
	if cfg.Database == "" {
		logger.Fatal("Database does not exist")
	}

	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}

	if cfg.Port == "" {
		cfg.Port = "3306"
	}

	if cfg.Username == "" {
		cfg.Username = "root"
	}

	if cfg.Charset == "" {
		cfg.Charset = "utf8"
	}

	return cfg
}
