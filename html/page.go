package html

import (
	"bufio"
	"db-snapshot/util"
	"fmt"
	"github.com/andybalholm/brotli"
	"github.com/gookit/slog"
	"os"
	"path/filepath"
	"strings"
)

type Html struct {
	Head1  string
	Head2  string
	Tables []string
}

func (self *Html) AddHead1(createTime string, instId int, host string, port int, dbName *string) {
	var page string
	page += fmt.Sprintf("%s\n", cssText)
	page += fmt.Sprintf("<h2>当前时间: %s</h2>\n", createTime)
	page += fmt.Sprintf("<h2>实例ID: %d</h2>\n", instId)
	if dbName != nil {
		page += fmt.Sprintf("<h2>IP端口: %s:%d/%s</h2><br>\n", host, port, *dbName)
	} else {
		page += fmt.Sprintf("<h2>IP端口: %s:%d</h2><br>\n", host, port)
	}

	self.Head1 = page
}

func (self *Html) AddHead2(title []string, data []int) {
	var th []string
	for _, v := range title {
		th = append(th, fmt.Sprintf("<th>%s</th>", v))
	}

	var td []string
	for _, v := range data {
		td = append(td, fmt.Sprintf("<td>%d</td>", v))
	}

	tr := fmt.Sprintf("<tr>%s</tr>\n", strings.Join(th, " "))
	tr += fmt.Sprintf("<tr>%s</tr>\n", strings.Join(td, " "))

	self.Head2 = fmt.Sprintf("<table>%s </table><br>\n", tr)
}

func (self *Html) AddHeadWithHref(title []string, refIds []string, data []int) {
	var th []string
	for _, v := range title {
		th = append(th, fmt.Sprintf("<th>%s</th>", v))
	}

	var td []string
	for i := range data {
		if refIds[i] == "" {
			td = append(td, fmt.Sprintf("<td>%d</td>", data[i]))
		} else {
			td = append(td, fmt.Sprintf(`<td><a href="#%s">%d</a></td>`, refIds[i], data[i]))
		}

	}

	tr := fmt.Sprintf("<tr>%s</tr>\n", strings.Join(th, " "))
	tr += fmt.Sprintf("<tr>%s</tr>\n", strings.Join(td, " "))

	self.Head2 = fmt.Sprintf("<table>%s </table><br>\n", tr)
}

func (self *Html) AddTable(title string, fieldNames []string, data [][]string) {

	var tableTitle, tableHead, tableBody string

	tableTitle = fmt.Sprintf("<h2>%s</h2>\n", title)
	for _, fieldname := range fieldNames {
		tableHead += fmt.Sprintf("<th>%s</th>", fieldname)
	}
	tableHead = fmt.Sprintf("<tr>%s</tr>\n", tableHead)

	for _, row := range data {
		tr := ""
		for _, val := range row {
			tr += fmt.Sprintf("<td>%s</td>", val)
		}
		tableBody += fmt.Sprintf("<tr>%s</tr>\n", tr)
	}
	text := fmt.Sprintf("<table>\n%s\n%s\n%s</table><br>", tableTitle, tableHead, tableBody)
	self.Tables = append(self.Tables, text)
}

func (self *Html) AddTableWithClassID(title string, classId string, fieldNames []string, data [][]string) {

	var tableTitle, tableHead, tableBody string

	tableTitle = fmt.Sprintf(`<h2 id="%s">%s</h2>
`, classId, title)
	for _, fieldname := range fieldNames {
		tableHead += fmt.Sprintf("<th>%s</th>", fieldname)
	}
	tableHead = fmt.Sprintf("<tr>%s</tr>\n", tableHead)

	for _, row := range data {
		tr := ""
		for _, val := range row {
			tr += fmt.Sprintf("<td>%s</td>", val)
		}
		tableBody += fmt.Sprintf("<tr>%s</tr>\n", tr)
	}
	text := fmt.Sprintf("<table>\n%s\n%s\n%s</table><br>", tableTitle, tableHead, tableBody)
	self.Tables = append(self.Tables, text)
}

func (self *Html) AddTableWithClassIDAndRowHref(title string, classId string, fieldNames []string, data [][]string, IdIndexes []int) {

	var tableTitle, tableHead, tableBody string

	tableTitle = fmt.Sprintf(`<h2 id="%s">%s</h2>
`, classId, title)
	for _, fieldname := range fieldNames {
		tableHead += fmt.Sprintf("<th>%s</th>", fieldname)
	}
	tableHead = fmt.Sprintf("<tr>%s</tr>\n", tableHead)

	for _, row := range data {
		tr := ""
		for idx, val := range row {
			var td string
			if val != "" && val != "NULL" && util.InSlice(idx, IdIndexes) {
				td = fmt.Sprintf(`<td><a href="#%s">%s</a></td>`, val, val)
			} else {
				td = fmt.Sprintf("<td>%s</td>", val)
			}

			tr += td
		}
		tableBody += fmt.Sprintf("<tr>%s</tr>\n", tr)
	}
	text := fmt.Sprintf("<table>\n%s\n%s\n%s</table><br>", tableTitle, tableHead, tableBody)
	self.Tables = append(self.Tables, text)
}

// index 表示第几列作为class id
func (self *Html) AddTableRowWithClassID(title string, fieldNames []string, data [][]string, IdIndex int) {

	var tableTitle, tableHead, tableBody string

	tableTitle = fmt.Sprintf(`<h2>%s</h2>
`, title)
	for _, fieldname := range fieldNames {
		tableHead += fmt.Sprintf("<th>%s</th>", fieldname)
	}
	tableHead = fmt.Sprintf("<tr>%s</tr>\n", tableHead)

	for _, row := range data {
		tr := ""
		for idx, val := range row {
			if idx == IdIndex {
				tr += fmt.Sprintf(`<td id="%s">%s</td>`, val, val)
			} else {
				tr += fmt.Sprintf("<td>%s</td>", val)
			}

		}
		tableBody += fmt.Sprintf("<tr>%s</tr>\n", tr)
	}
	text := fmt.Sprintf("<table>\n%s\n%s\n%s</table><br>", tableTitle, tableHead, tableBody)
	self.Tables = append(self.Tables, text)
}

func (self *Html) Save(dirname, filename string) {
	//保存

	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		//目录不存在，创建目录
		err := os.MkdirAll(dirname, 0775)
		if err != nil {
			slog.Errorf("创建目录%s报错: %s", dirname, err)
		}
	}

	f, err := os.OpenFile(dirname+filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		slog.Error(err)
	}
	defer f.Close()

	f.WriteString(self.Head1)
	f.WriteString(self.Head2)

	for _, v := range self.Tables {
		f.WriteString(v + "\n")
	}

}

func (self *Html) SaveToBrotli(dirname, filename string) error {
	fullPath := filepath.Join(dirname, filename+".br")

	// 创建目录
	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		err := os.MkdirAll(dirname, 0775)
		if err != nil {
			return fmt.Errorf("创建目录失败: %w", err)
		}
	}

	// 创建文件
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	// file (磁盘) <- bufio (缓冲区) <- brWriter (压缩器)
	buf := bufio.NewWriterSize(file, 64*1024) // 设置 64KB 缓冲区
	brWriter := brotli.NewWriterLevel(buf, brotli.DefaultCompression)

	// 写入数据
	if _, err := brWriter.Write([]byte(self.Head1)); err != nil {
		return err
	}
	if _, err := brWriter.Write([]byte(self.Head2)); err != nil {
		return err
	}

	for _, v := range self.Tables {
		brWriter.Write([]byte(v))
		brWriter.Write([]byte("\n"))
	}

	// 先关闭压缩器（写入结尾标记）
	if err := brWriter.Close(); err != nil {
		return fmt.Errorf("brotli close 失败: %w", err)
	}
	// 再刷新缓冲区（真正写入磁盘）
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("bufio flush 失败: %w", err)
	}

	return nil
}
