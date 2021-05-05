package main

import (
	"async-comm/pkg/asynccomm"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type App struct {
	aclib *asynccomm.AsyncComm
	cnf   *Config
	log   Logger
}

func NewApp(aclib *asynccomm.AsyncComm, cnf *Config, log Logger) *App {
	return &App{
		aclib: aclib,
		cnf:   cnf,
		log:   log,
	}
}

func GetCurTime() string {
	t := time.Now()
	milli := fmt.Sprintf("%d", t.UnixNano() / int64(time.Millisecond))
	return fmt.Sprintf("%02d:%02d:%02d:%02d:%02d.%02s", t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), milli[len(milli)-3:])
}

func GetTimeFromString(msgTime string) time.Time {
	defer func() {
		if ok := recover(); ok != nil {
			log.Errorf("PanicRecoverd: failed to parse time '%s': %s", msgTime, ok)
		}
	}()
	loc, _ := time.LoadLocation("Asia/Kolkata")
	prts := strings.Split(msgTime, ":")
	mnth, _ := strconv.Atoi(prts[0])
	day, _ := strconv.Atoi(prts[1])
	hr, _ := strconv.Atoi(prts[2])
	min, _ := strconv.Atoi(prts[3])
	secPrts := strings.Split(prts[4], ".")
	sec, _ := strconv.Atoi(secPrts[0])
	mill, _ := strconv.Atoi(secPrts[1])
	t := time.Date(time.Now().Year(), time.Month(mnth), day, hr, min, sec, 0, loc)
	t.Add(time.Duration(mill) * time.Millisecond)
	return t
}

type ProducerMessage struct {
	App string
	Producer string
	Time string
}
/**
 * custom error types
 **/
// MessageFormatError is type error for specifying any errors
// encountered during the template parsing of message
type MessageFormatError struct {
	Message string
	Description string
}

func (m *MessageFormatError) Error() string {
	return fmt.Sprintf("error: %s, description: %s", m.Message, m.Message)
}