package utils

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	gopath = filepath.Join(os.Getenv("GOPATH"), "src") + "/"
)

var (
	TablesNotFoundErr = errors.New("Tables not fit")
	TablesNotInitErr  = errors.New("Table not init")
	KeyFormatErr      = errors.New("Key format not match")
	TableCompactErr   = errors.New("Table Compact failed")
	TopTablesPullErr  = errors.New("Top Tables Pull failed")
)

func CondPanic(condition bool, e error) {
	if condition {
		panic(e)
	}
}

func Panic(e error) {
	if e != nil {
		panic(e)
	}
}

func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

/*获取当前代码运行时位置*/
func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep) /*找到上deep层的代码执行信息*/
	if !ok {
		file = "???"
		line = 0
	}
	if fullPath { /*我们要获取全路径*/
		if strings.HasSuffix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}
