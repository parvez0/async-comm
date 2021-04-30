package asynccommtest

import (
	"async-comm/internal/app/asynccommtest/config"
	"async-comm/internal/app/asynccommtest/logger"
	"async-comm/pkg/redis"
	"fmt"
	"time"
)

// asyncomm
// config
// ac_logger

rdb := redis.NewRdb(context.TODO(), cnf.Redis, log, 0)
defer rdb.Close()

func (a *App) InitializeApp(rdb *redis.Redis, log logger.Logger, cnf *config.Config)  {
	a.Rdb = rdb
	a.log = log
	a.cnf = cnf
}

func (a *App) RegisterConsumer(r config.Routine)  {
	sts, err := a.Rdb.Set("consumer_" + r.Name, time.Now().String(), time.Duration(r.RefreshTime) * time.Millisecond)
	if err != nil {
		a.log.Panicf("failed to register consumer %s - %s", r.Name, err.Error())
	}
	a.log.Infof("consumer '%s' registered with refreshTime: '%dms', status: '%s'", r.Name, r.RefreshTime, sts)
}

func (a *App) Pull(r config.Routine)  {
	ch := make(chan redis.Packet, 10)
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

func (a *App) Push(r *config.Routine) error {
	var err error
	// parsing the message from the format provided into a string using go templates
	r.Message.FormattedMsg, err = ParseTemplate(r, a.cnf.Server.App)
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
