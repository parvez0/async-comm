package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

// Logger provides an interface to convert
// logger to custom logger type, it will have
// all the basic functionalities of a logger
type Logger interface {
	Info(args ...interface{})
	Warn(args ...interface{})
	Debug(args ...interface{})
	Error(args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
}

var log *logrus.Logger

// NewLogger returns a logrus custom_logger object with prefilled options
func InitializeLogger(level, outFilePath string) (Logger, error) {
	if log != nil {
		return log, nil
	}

	baseLogger := logrus.New()

	// set REQUESTS_LOGLEVEL for custom_logger level, defaults to info
	logLevel, err := logrus.ParseLevel(level)
	if err != nil{
		return nil, fmt.Errorf("failed to parse log level : %s", err.Error())
	}

	// setting custom_logger format to string
	baseLogger.SetFormatter(&Formatter{
		TimestampFormat: "2006-01-02 15:04:05",
		LogFormat:       "[%lvl%]: %time% -- [ AsyncComm ] -- - %msg%\n",
	})

	// set to true for showing filename and line number from where custom_logger being called
	baseLogger.SetReportCaller(false)
	baseLogger.SetLevel(logLevel)

	// directing log output to a file if OutfilePath is defined, by default it will log to stdout
	if outFilePath != "" {
		file := filepath.Clean(outFilePath)
		fd, err := os.OpenFile(file, os.O_WRONLY | os.O_CREATE, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s for logging - %s", file, err.Error())
		}
		baseLogger.SetOutput(fd)
	}
	log = baseLogger
	return log, nil
}

func SetLevel(level string)  (Logger, error) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil{
		return nil, fmt.Errorf("failed to parse log level : %s", err.Error())
	}
	log.SetLevel(lvl)
	return log, nil
}