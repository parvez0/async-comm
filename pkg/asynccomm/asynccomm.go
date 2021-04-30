package main

import (
	act "async-comm/internal/app/asynccommtest"
	"async-comm/internal/app/asynccommtest/config"
	"async-comm/internal/app/asynccommtest/logger"
	"async-comm/pkg/redis"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var cnf *config.Config
var log logger.Logger

func init() {
	cnf = config.InitializeConfig()
	log = logger.InitializeLogger(cnf)
}

func main() {
	log.Infof("starting service %s", cnf.Server.App)

	rdb := redis.NewRdb(context.TODO(), cnf.Redis, log, 0)
	defer rdb.Close()

	app := &act.App{}
	app.InitializeApp(rdb, log, cnf)

	// quit channel inform all the go routines to exit after finishing it's processing
	quit := make(chan bool, 1)
	wg := sync.WaitGroup{}

	// setting up routines for producers and consumers
	for _, r := range cnf.Server.Routines {
		switch r.Role {
		case "producer":
			wg.Add(1)
			// creating producer routine to send messages at every given interval
			go app.InitiateProducer(r, app, &wg, quit)
		case "consumer":
			app.RegisterConsumer(r)
			go app.InitiateConsumers(r, app, &wg)
		}
	}

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)
	<-termChan
	quit <- true
	wg.Wait()
}