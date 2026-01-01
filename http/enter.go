package http

import (
	"embed"
	"fmt"
	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func StartService(db *gorm.DB, port int, embedFS embed.FS) {

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

	staticFS, err := fs.Sub(embedFS, "web/static")
	if err != nil {
		panic(fmt.Sprintf("Failed to subset embedded files: %v", err))
	}

	// 首页
	r.GET("/", func(c *gin.Context) {
		data, err := embedFS.ReadFile("web/index.html")
		if err != nil {
			c.String(404, "File index.html not found")
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", data)
	})

	// 路由分组
	root := r.Group("/db-snapshot")
	{

		// API 分组
		api := root.Group("/api")
		{
			api.GET("/snapshotList", GetDBSnapshotList(db))

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

		// 将 web/static 映射到 /db-snapshot/static
		root.StaticFS("/static", http.FS(staticFS))

		//root.Static("/data", "./data")
		root.GET("/data/:date/:id/:filename", func(c *gin.Context) {
			date := c.Param("date")
			id := c.Param("id")
			filename := c.Param("filename") + ".br"

			filePath := filepath.Join("data", date, id, filename)

			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				c.String(http.StatusNotFound, "File not found")
				return
			}

			// 检查浏览器是否支持 br
			acceptEncoding := c.GetHeader("Accept-Encoding")
			if !strings.Contains(acceptEncoding, "br") {
				// 不支持 br的浏览器
				file, _ := os.Open(filePath)
				if err != nil {
					c.String(http.StatusNotFound, err.Error())
					return
				}
				defer file.Close()

				// 创建解压器
				brReader := brotli.NewReader(file)
				c.Header("Content-Type", "text/html; charset=utf-8")
				// 使用 io.Copy 将解压后的流直接写回响应
				io.Copy(c.Writer, brReader)

				return
			}

			c.Header("Content-Type", "text/html; charset=utf-8")
			c.Header("Content-Encoding", "br")
			c.Header("Cache-Control", "public, max-age=60") // 缓存一分钟

			c.File(filePath)

		})

		root.GET("/config", func(c *gin.Context) {
			data, err := embedFS.ReadFile("web/config.html")
			if err != nil {
				c.String(404, "File config.html not found")
				return
			}
			c.Data(http.StatusOK, "text/html; charset=utf-8", data)
		})

		root.GET("/dashboard/*any", func(c *gin.Context) {
			data, err := embedFS.ReadFile("web/dashboard.html")
			if err != nil {
				c.String(404, "File dashboard.html not found")
				return
			}
			c.Data(http.StatusOK, "text/html; charset=utf-8", data)
		})

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
