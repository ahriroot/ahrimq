package v1

type Config struct {
	Host         string
	Port         int
	AccessKey    string
	AccessSecret string
}

func NewConfig() *Config {
	return &Config{
		Host:         "127.0.0.1",
		Port:         60001,
		AccessKey:    "",
		AccessSecret: "",
	}
}
