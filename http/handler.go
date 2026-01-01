package http

import (
	"db-snapshot/config"
	"db-snapshot/model"
	"db-snapshot/util"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"time"
)

type QueryParams struct {
	InstID    int64   `form:"inst_id" binding:"required"`
	StartTime *string `form:"start_time"`
	EndTime   *string `form:"end_time"`
}

func GetDBSnapshotList(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var q QueryParams
		if err := c.ShouldBindQuery(&q); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: " + err.Error()})
			return
		}

		if q.InstID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: inst_id"})
			return
		}

		var err error
		var start, end time.Time
		if q.StartTime == nil {
			start = time.Now().Add(-time.Hour * 24)
		} else {
			start, err = time.ParseInLocation("2006-01-02 15:04:05", *q.StartTime, time.Local)
			if err != nil {
				c.Error(err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "start_time 格式错误"})
				return
			}
		}

		if q.EndTime == nil {
			end = time.Now()
		} else {
			end, err = time.ParseInLocation("2006-01-02 15:04:05", *q.EndTime, time.Local)
			if err != nil {
				c.Error(err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "end_time 格式错误"})
				return
			}
		}

		// 构造查询
		var list []model.DBSnapshot
		err = db.Where("inst_id = ?", q.InstID).
			Where("create_time BETWEEN ? AND ?", start, end).
			Order("create_time").
			Find(&list).Error

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, list)
	}
}

func ListConfig(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		dbType := c.Query("db_type")

		query := db.Model(&model.DBSnapshotConfig{})

		if dbType != "" {
			query = query.Where("db_type = ?", dbType)
		}

		var list []model.DBSnapshotConfig
		if err := query.Order("inst_id").Find(&list).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, list)
	}
}

func GetConfig(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		instID := c.Param("inst_id")

		var cfg model.DBSnapshotConfig
		if err := db.First(&cfg, "inst_id = ?", instID).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "记录不存在"})
				return
			}
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, cfg)
	}
}

func CreateConfig(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req model.DBSnapshotConfig

		if err := c.ShouldBindJSON(&req); err != nil {
			c.Error(err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误"})
			return
		}

		if err := db.Create(&req).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"inst_id": req.InstID})
	}
}

func DeleteConfig(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		instID := c.Param("inst_id")

		if err := db.Delete(&model.DBSnapshotConfig{}, "inst_id = ?", instID).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"deleted": true})
	}
}

func UpdateConfig(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		instID := c.Param("inst_id")

		var req model.DBSnapshotConfig
		if err := c.ShouldBindJSON(&req); err != nil {
			c.Error(err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误"})
			return
		}

		// 只更新有效字段
		if err := db.Model(&model.DBSnapshotConfig{}).
			Where("inst_id = ?", instID).
			Updates(req).Error; err != nil {
			c.Error(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"updated": true})
	}
}

func ReloadConfigHandler(c *gin.Context) {
	config.Global.ReloadConfigChan <- struct{}{}
	c.JSON(http.StatusOK, gin.H{"msg": "收到重载配置请求"})
}

func TestConnectionHandler(c *gin.Context) {

	var req model.DBSnapshotConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: " + err.Error()})
		return
	}

	if req.DBType == "pgsql" {
		req.DBName = "postgres"
	}

	cfg := &model.DBConfig{
		Host:     req.Host,
		Port:     req.Port,
		User:     config.Global.MonitorUser,
		Password: config.Global.MonitorPassword,
		Database: req.DBName,
	}

	err := util.PingDB(req.DBType, cfg)
	if err != nil {
		c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "连接失败: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": "连接成功！"})
}
