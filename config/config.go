package config

import (
	"db-snapshot/model"
	"github.com/go-ini/ini"
	"github.com/gookit/slog"
)

var Global *Config

type Config struct {
	HttpPort         int            `ini:"http_port"`
	Interval         int            `ini:"interval"`
	Parallel         int            `ini:"parallel"`
	MonitorUser      string         `ini:"monitor_user"`
	MonitorPassword  string         `ini:"monitor_password"`
	DB               model.DBConfig `ini:"db"`
	ReloadConfigChan chan struct{}
}

func init() {
	fileName := `config.ini`

	//加载配置文件
	c, err := ini.Load(fileName)
	if err != nil {
		slog.Fatalf("加载配置文件 '%s' 失败: %v", fileName, err)
		return
	}

	Global = new(Config)
	Global.ReloadConfigChan = make(chan struct{}, 100)
	err = c.MapTo(Global)
	if err != nil {
		slog.Fatalf("映射配置信息失败 %v", err)
		return
	}

	if Global.HttpPort == 0 {
		Global.HttpPort = 8080
	}

	if Global.Parallel == 0 {
		Global.Parallel = 8
	}

	if Global.Interval == 0 {
		Global.Parallel = 30
	}
}
