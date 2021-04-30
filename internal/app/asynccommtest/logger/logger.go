package logger

import (
	"async-comm/internal/app/asynccommtest/config"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"sync"
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
var mutex = sync.Mutex{}

// NewLogger returns a logrus custom_logger object with prefilled options
func InitializeLogger(config *config.Config) Logger {
	if log != nil {
		return log
	}

	mutex.Lock()
	defer mutex.Unlock()

	baseLogger := logrus.New()

	// set REQUESTS_LOGLEVEL for custom_logger level, defaults to info
	level, err := logrus.ParseLevel(config.Logger.Level)
	if err != nil{
		panic(fmt.Sprintf("failed to parse log level : %s", err.Error()))
	}

	// setting custom_logger format to string
	baseLogger.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		FullTimestamp: config.Logger.FullTimestamp,
	})

	// set to true for showing filename and line number from where custom_logger being called
	baseLogger.SetReportCaller(false)
	baseLogger.SetLevel(level)

	// directing log output to a file if OutfilePath is defined, by default it will log to stdout
	if config.Logger.OutputFilePath != "" {
		file := filepath.Clean(config.Logger.OutputFilePath)
		fd, err := os.OpenFile(file, os.O_WRONLY | os.O_CREATE, 0755)
		if err != nil {
			log.Panicf("failed to open file %s for logging - %s", file, err.Error())
		}
		baseLogger.SetOutput(fd)
	}
	log = baseLogger
	return log
}
