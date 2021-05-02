package src

import (
	"asynccomm-lib/asynccomm"
	"fmt"
	"time"
)

type App struct {
	aclib *asynccomm.AsyncComm
	cnf *Config
	log Logger
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
	return fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
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