package util

import (
	"fmt"
	"github.com/gookit/slog"
	"os"
	"path/filepath"
	"time"
)

func EnterWorkDir() {
	fullpath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir, _ := filepath.Split(fullpath)
	err = os.Chdir(dir)
	if err != nil {
		panic(err)
	}
	currentDir, _ := os.Getwd()
	fmt.Printf("当前目录为: %s\n", currentDir)
}
func TimeCost() func(str string) {
	//计算耗时
	bts := time.Now().Unix()
	return func(str string) {
		ts := time.Now().Unix() - bts
		slog.Infof("%s，耗时%ds", str, ts)
	}
}

func InSlice[T comparable](target T, list []T) bool {
	for i, _ := range list {
		if target == list[i] {
			return true
		}
	}
	return false
}

func GetManyOfChan[T any](ch *chan T, size int) (rows []T) {
	if size == 0 {
		panic("size==0")
	}
	for i := 0; i < size; i++ {
		row, ok := <-*ch
		if ok {
			rows = append(rows, row)
		} else {
			break
		}
	}
	return rows
}
