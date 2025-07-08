package v1

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Mode string

const (
	Active  Mode = "active"
	Passive Mode = "passive"
)

type Config struct {
	Path         string
	Host         string
	Port         int
	AccessKey    string
	AccessSecret string
	Mode         Mode
	PingInterval time.Duration
}

func NewConfig() *Config {
	return &Config{
		Path:         "",
		Host:         "127.0.0.1",
		Port:         60001,
		AccessKey:    "",
		AccessSecret: "",
		Mode:         Active,
		PingInterval: 60 * time.Second,
	}
}

func (c *Config) GetAddress() string {
	addr := net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port))
	return addr
}

func (c *Config) GetUnixPath() string {
	if strings.HasPrefix(c.Path, "./") {
		current_dir, _ := os.Getwd()
		abs_path := filepath.Join(current_dir, c.Path)
		return abs_path
	} else if strings.HasPrefix(c.Path, "~") {
		home_dir, _ := os.UserHomeDir()
		abs_path := filepath.Join(home_dir, c.Path[2:])
		return abs_path
	} else {
		return c.Path
	}
}

func (c *Config) GetPipePath() string {
	path := strings.TrimSpace(c.Path)

	// Windows 命名管道标准格式: \\.\pipe\pipename
	if strings.HasPrefix(path, `\\.\pipe\`) {
		return path
	}

	// 处理相对路径
	if strings.HasPrefix(path, "./") {
		currentDir, _ := os.Getwd()
		absPath := filepath.Join(currentDir, path[2:])
		return absPath
	}

	// 处理用户目录路径
	if strings.HasPrefix(path, "~") {
		homeDir, _ := os.UserHomeDir()
		absPath := filepath.Join(homeDir, path[2:])
		return absPath
	}

	// 自动添加命名管道前缀
	if !strings.HasPrefix(path, "pipe\\") && !strings.HasPrefix(path, "pipe/") {
		if strings.HasPrefix(path, "\\") || strings.HasPrefix(path, "/") {
			return `\\.\pipe\` + path[1:]
		}
		return `\\.\pipe\` + path
	}

	return path
}
