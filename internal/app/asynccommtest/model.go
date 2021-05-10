package main

import (
	"async-comm/pkg/asynccomm"
	"fmt"
	"regexp"
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

func FormatTime(t time.Time) string {
	milli := fmt.Sprintf("%d", t.UnixNano() / int64(time.Millisecond))
	return fmt.Sprintf("%02d:%02d:%02d:%02d:%02d.%02s", t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), milli[len(milli)-3:])
}

func GetCurTime() string {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Errorf("failed to load location 'Asia/Kolkata': %s", err.Error())
	}
	t := time.Now().In(loc)
	return FormatTime(t)
}

func GetTimeFromString(msg string) (t time.Time) {
	defer func() {
		if ok := recover(); ok != nil {
			log.Errorf("PanicRecoverd: failed to parse time '%s': %s", msg, ok)
			t = time.Now()
		}
	}()
	r := regexp.MustCompile("\\d{2}:\\d{2}:\\d{2}:\\d{2}:\\d{2}\\.\\d{3}")
	msgTime := r.FindString(msg)
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Errorf("failed to load location 'Asia/Kolkata': %s", err.Error())
	}
	prts := strings.Split(msgTime, ":")
	mnth, _ := strconv.Atoi(prts[0])
	day, _ := strconv.Atoi(prts[1])
	hr, _ := strconv.Atoi(prts[2])
	min, _ := strconv.Atoi(prts[3])
	secPrts := strings.Split(prts[4], ".")
	sec, _ := strconv.Atoi(secPrts[0])
	mill, _ := strconv.Atoi(secPrts[1])
	ns := int(time.Duration(mill)*time.Millisecond)
	t = time.Date(time.Now().Year(), time.Month(mnth), day, hr, min, sec, ns, loc)
	log.Warnf("prts: %+v, sec: %d, mill: %d, nanoSc: %d, time: %s", prts, sec, mill, ns, FormatTime(t))
	return t
}

type Message struct {
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