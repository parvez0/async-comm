package api

import (
	"async-comm/pkg"
	"fmt"
	"log"
	"time"
)

type App struct {
	Rdb *pkg.Redis
	cnf *pkg.Config
	log pkg.Logger
}

func (a *App) InitializeApp(rdb *pkg.Redis, log pkg.Logger, cnf *pkg.Config)  {
	a.Rdb = rdb
	a.log = log
	a.cnf = cnf
}

func (a *App) RegisterConsumer(r pkg.Routine)  {
	sts, err := a.Rdb.Set("consumer_" + r.Name, time.Now().String(), time.Duration(r.RefreshTime) * time.Millisecond)
	if err != nil {
		log.Panicf("failed to register consumer %s - %s", r.Name, err.Error())
	}
	a.log.Infof("consumer '%s' registered with refreshTime: '%dms', status: '%s'", r.Name, r.RefreshTime, sts)
}

func (a *App) Push(r *pkg.Routine) error {
	var err error
	// parsing the message from the format provided into a string using go templates
	r.Message.FormattedMsg, err = pkg.ParseTemplate(r, a.cnf.Server.App)
	if err != nil {
		errMsg := MessageFormatError{
			Message:  "Bad Message format",
			Description: fmt.Sprintf("failed to parse message '%s' for producer %s - %s", r.Message.Format, r.Name, err.Error()),
		}
		return &errMsg
	}
	_, err = a.Rdb.Produce(r.Q, r.Message.FormattedMsg)
	if err != nil {
		return fmt.Errorf("failed to publish '%s' from producer '%s' - error: %s", r.Message.FormattedMsg, r.Name, err.Error())
	}
	return nil
}

func (a *App) Pull(r pkg.Routine, quit chan bool)  {
	ch := make(chan pkg.Packet, 10)
	go a.Rdb.Consume(r)
	for {
		select {
			case msg := <- ch:
				a.log.Infof("message consumed Id: %s, Message : %+v", msg.Id, msg.Message)
				err := a.Rdb.Ack(r, 1, msg.Id)
				if err != nil {
					a.log.Errorf("failed to acknowledge message %s - %s", msg.Id, err.Error())
				}
		}
	}
}

// MessageFormatError is type error for specifying any errors
// encountered during the template parsing of message
type MessageFormatError struct {
	Message string
	Description string
}

func (m *MessageFormatError) Error() string {
	return fmt.Sprintf("error: %s, description: %s", m.Message, m.Message)
}
