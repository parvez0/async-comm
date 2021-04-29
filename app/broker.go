package app

import (
	"async-comm/pkg"
	"log"
	"time"
)

type App struct {
	Rdb *pkg.Redis
	log pkg.Logger
}

func (a *App) InitializeApp(rdb *pkg.Redis, log pkg.Logger)  {
	a.Rdb = rdb
	a.log = log
}

func (a *App) RegisterConsumer(r pkg.Routine)  {
	sts, err := a.Rdb.Set("consumer_" + r.Name, time.Now().String(), time.Duration(r.RefreshTime) * time.Millisecond)
	if err != nil {
		log.Panicf("failed to register consumer %s - %s", r.Name, err.Error())
	}
	a.log.Infof("consumer '%s' registered with refreshTime: '%dms', status: '%s'", r.Name, r.RefreshTime, sts)
}

func (a *App) Push(qName, msg string) error {
	return a.Rdb.Produce(qName, msg)
}

func (a *App) Pull(r pkg.Routine)  {
	ch := make(chan pkg.Packet, 10)
	go a.Rdb.Consume(ch, r)
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
