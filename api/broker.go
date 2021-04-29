package api

import (
	"async-comm/pkg"
)

var log = pkg.InitializeLogger()

type App struct {
	Rdb *pkg.Redis
}

func (a *App) InitializeApp(rdb *pkg.Redis)  {
	a.Rdb = rdb
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
				log.Infof("message consumed Id: %s, Message : %+v", msg.Id, msg.Message)
		}
	}
}
