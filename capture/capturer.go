package capture

import (
	"db-snapshot/config"
	"db-snapshot/engine/mysql"
	"db-snapshot/engine/oceanbase"
	"db-snapshot/engine/oracle"
	"db-snapshot/engine/pgsql"
	"db-snapshot/model"
	"fmt"
	"github.com/gookit/slog"
	"gorm.io/gorm"
)

type Capturer interface {
	Init() error
	Close()
	Run(db *gorm.DB)
}

func NewCapturer(inst model.Instance) Capturer {
	switch inst.DBType {
	case "mysql", "polar", "tdsqlc":
		return &mysql.Engine{
			InstID: inst.InstId, Name: fmt.Sprintf("%s:%d", inst.Host, inst.Port),
			Cfg: model.DBConfig{Host: inst.Host, Port: inst.Port, User: config.Global.MonitorUser, Password: config.Global.MonitorPassword, Database: "information_schema"},
		}
	case "pgsql":
		return &pgsql.Engine{
			InstID: inst.InstId, Name: fmt.Sprintf("%s:%d", inst.Host, inst.Port),
			Cfg: model.DBConfig{Host: inst.Host, Port: inst.Port, User: config.Global.MonitorUser, Password: config.Global.MonitorPassword, Database: "postgres"},
		}
	case "oracle":
		return &oracle.Engine{
			InstID: inst.InstId, Name: fmt.Sprintf("%s:%d", inst.Host, inst.Port),
			Cfg: model.DBConfig{Host: inst.Host, Port: inst.Port, User: config.Global.MonitorUser, Password: config.Global.MonitorPassword, Database: inst.DBName},
		}
	case "oceanbase":
		return &oceanbase.Engine{
			InstID: inst.InstId, Name: fmt.Sprintf("%s:%d", inst.Host, inst.Port),
			Cfg: model.DBConfig{Host: inst.Host, Port: inst.Port, User: config.Global.MonitorUser, Password: config.Global.MonitorPassword, Database: "information_schema"},
		}

	default:
		slog.Errorf("[%s] 不支持该数据库类型: %s", fmt.Sprintf("%s:%d", inst.Host, inst.Port), inst.DBType)
		return nil
	}

}
