package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
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
var fd *os.File

// NewLogger returns a logrus custom_logger object with prefilled options
func InitializeLogger(config *Config) Logger {
	if log != nil {
		return log
	}
	absPath := ""
	fmt.Println("Output File Path: ", config.Logger.OutputFilePath)
	if config.Logger.OutputFilePath != "" {
		var err error
		absPath, err = filepath.Abs(config.Logger.OutputFilePath)
		if err != nil {
			panic(fmt.Errorf("failed to load logfile : %s", err.Error()))
		}
		fmt.Println("Abs Path: ", absPath)
		path := strings.Split(absPath, "/")
		_, err = os.Stat(strings.Join(path[:len(path)-1], "/"))
		if err != nil {
			panic(fmt.Errorf("failed to load logfile : %s", err.Error()))
		}
	}

	baseLogger := logrus.New()

	// set REQUESTS_LOGLEVEL for custom_logger level, defaults to info
	level, err := logrus.ParseLevel(config.Logger.Level)
	if err != nil {
		panic(fmt.Sprintf("failed to parse log level : %s", err.Error()))
	}

	// setting custom_logger format to string
	baseLogger.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		FullTimestamp: config.Logger.FullTimestamp,
	})

	// directing log output to a file if OutfilePath is defined, by default it will log to stdout
	if config.Logger.OutputFilePath != "" {
		fd, err = os.OpenFile(absPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
		if err != nil {
			fmt.Printf("failed to open file %s for logging - %s", absPath, err.Error())
			os.Exit(1)
		}
		baseLogger.SetOutput(fd)
	}

	// set to true for showing filename and line number from where custom_logger being called
	baseLogger.SetReportCaller(false)
	baseLogger.SetLevel(level)

	log = baseLogger
	return log
}

func CloseLogger() {
	if fd != nil {
		log.Debugf("flusing pending logs to file and close the descriptor")
		fd.Sync()
		fd.Close()
	}
}
