// short for "log"
package lg

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

type Logger interface {
	Output(maxdepth int, s string) error
}

type NilLogger struct{}

func (l NilLogger) Output(maxdepth int, s string) error {
	return nil
}

type LogLevel int

//Get方法也是必须的，当你把一个自定义变量绑定在flag上时，需要设还需要取。
func (l *LogLevel) Get() interface{} { return *l }

// Set是一个用来设置flag值的方法,
// Set的参数是String类型，用于设置自定义变量l
func (l *LogLevel) Set(s string) error {
	lvl, err := ParseLogLevel(s)
	if err != nil {
		return err
	}
	*l = lvl
	return nil
}

// String是一个用来读取这个自定义flag变量的某些值,具体读什么由自己决定。
//我们常常在使用某些基本类型变量的时候经常会调用默认的String()函数，比如config:=flagSet.Lookup("config").Value.String()
//只不过flag对于自定义变量,需要自己手动实现String()函数。
//但是有个问题，String()函数只能返回字符串类型的值，对于绑定的bool类型，flagSet.Lookup("config").Value.Bool()没有这样的操作方法，所以这时候要用到Getter,其内部实现了Get函数。
func (l *LogLevel) String() string {
	switch *l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARNING"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	}
	return "invalid"
}

func ParseLogLevel(levelstr string) (LogLevel, error) {
	switch strings.ToLower(levelstr) {
	case "debug":
		return DEBUG, nil
	case "info":
		return INFO, nil
	case "warn":
		return WARN, nil
	case "error":
		return ERROR, nil
	case "fatal":
		return FATAL, nil
	}
	return 0, fmt.Errorf("invalid log level '%s' (debug, info, warn, error, fatal)", levelstr)
}

func Logf(logger Logger, cfgLevel LogLevel, msgLevel LogLevel, f string, args ...interface{}) {
	if cfgLevel > msgLevel { //越高是越严重的错误，表示若没有达到配置文件的日志输出等级要求，就不管。
		return
	}
	//logger就是opts.Logger，其实就是官方的日志对象。这个对象在前面第一次建立的时候指定了输出是stderr.
	logger.Output(3, fmt.Sprintf(msgLevel.String()+": "+f, args...)) //真正负责日志打印的方法，其它级别的打印方法都将会调用它
	//第2个参数calldepth用于恢复PC，出于一般性而提供，但目前在所有预定义的路径上它的值都为2
}

func LogFatal(prefix string, f string, args ...interface{}) {
	logger := log.New(os.Stderr, prefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	Logf(logger, FATAL, FATAL, f, args...)
	os.Exit(1)
}
