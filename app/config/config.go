package config

type Config struct {
	Dir        string
	DBFilename string
}

func New(dir, dbfilename string) *Config {
	return &Config{
		Dir:        dir,
		DBFilename: dbfilename,
	}
}

func (c *Config) Get(key string) string {
	switch key {
	case "dir":
		return c.Dir
	case "dbfilename":
		return c.DBFilename
	default:
		return ""
	}
}
