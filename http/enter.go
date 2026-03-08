package http

import (
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func StartService(db *gorm.DB, port int) {

	f, err := os.OpenFile("http.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		os.Exit(1)
	}
	gin.DefaultWriter = f
	gin.DefaultErrorWriter = f

	r := gin.Default()

	// 跨域中间件抽取
	r.Use(corsMiddleware())

	// 首页
	r.GET("/", func(c *gin.Context) {
		c.File("web/index.html")
	})

	// 路由分组
	root := r.Group("/db-snapshot")

	// 页面
	{
		// 静态资源
		root.Static("/static", "web/static")

		root.GET("/config", func(c *gin.Context) {
			c.File("web/config.html")
		})

		root.GET("/dashboard/*any", func(c *gin.Context) {
			c.File("web/dashboard.html")
		})

		root.GET("/mysql-snapshot", func(c *gin.Context) {
			c.File("web/snapshot_mysql.html")
		})

		root.GET("/oracle-snapshot", func(c *gin.Context) {
			c.File("web/snapshot_oracle.html")
		})

		root.GET("/pgsql-snapshot", func(c *gin.Context) {
			c.File("web/snapshot_pgsql.html")
		})

		root.GET("/oceanbase-snapshot", func(c *gin.Context) {
			c.File("web/snapshot_oceanbase.html")
		})
	}

	// API
	api := root.Group("/api")
	{
		api.GET("/snapshotList", GetDBSnapshotList(db))
		api.GET("/snapshot/:inst_id/:snapshot_id", GetSnapshotData)

		config := api.Group("/config")
		{
			config.POST("/", CreateConfig(db))
			config.GET("/", ListConfig(db))
			config.GET("/:inst_id", GetConfig(db))
			config.PUT("/:inst_id", UpdateConfig(db))
			config.DELETE("/:inst_id", DeleteConfig(db))
			config.POST("/ping", TestConnectionHandler)
			config.GET("/reload", ReloadConfigHandler)
		}
	}

	r.Run(fmt.Sprintf(":%d", port))
}

// 提取跨域中间件
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}
