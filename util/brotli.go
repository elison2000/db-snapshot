package util

import (
	"bufio"
	"github.com/andybalholm/brotli"
	"os"
)

func SaveToBrotli(filePath string, data []byte) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 使用 bufio 提高写入效率
	bufferedWriter := bufio.NewWriter(file)
	defer bufferedWriter.Flush()

	// 创建 Brotli Writer
	// brotli.DefaultCompression (级别 6) 是速度和压缩率的平衡点
	// brotli.BestCompression (级别 11) 压缩率最高但最耗 CPU
	brWriter := brotli.NewWriterLevel(bufferedWriter, brotli.DefaultCompression)
	defer brWriter.Close()

	// 写入数据
	_, err = brWriter.Write(data)
	if err != nil {
		return err
	}

	return nil
}
