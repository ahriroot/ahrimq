package v1

import "time"

type Mode string

const (
	Active  Mode = "active"
	Passive Mode = "passive"
)

type Config struct {
	Host         string
	Port         int
	AccessKey    string
	AccessSecret string
	Mode         Mode
	PingInterval time.Duration
}

func NewConfig() *Config {
	return &Config{
		Host:         "127.0.0.1",
		Port:         60001,
		AccessKey:    "",
		AccessSecret: "",
		Mode:         Active,
		PingInterval: 60 * time.Second,
	}
}
