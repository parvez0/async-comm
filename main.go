package main

import (
	"async-comm/app"
	"async-comm/pkg"
	"context"
	"os"
	"os/signal"
	"syscall"
)

var cnf *pkg.Config
var log pkg.Logger

func init() {
	cnf = pkg.InitializeConfig()
	log = pkg.InitializeLogger()
}

func main() {
	log.Infof("starting service %s", cnf.Server.App)

	rdb := pkg.NewRdb(context.TODO(), cnf.Redis, 0)
	app := app.App{}
	app.InitializeApp(rdb, log)

	// setting up routines for producers and consumers
	for _, r := range cnf.Server.Routines {
		switch r.Role {
		case "producer":
			go func() {
				for {
					pMsg, err := pkg.ParseTemplate(r, cnf.Server.App)
					if err != nil {
						log.Errorf("failed to parse message '%s' - %s", r.Message.Format, err.Error())
						continue
					}
					app.Push(r.Q, pMsg)
				}
			}()
		case "consumer":
			app.RegisterConsumer(r)
		}
	}

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan
}
