package main

import (
	"async-comm/api"
	"async-comm/pkg"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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
	defer rdb.Close()

	app := &api.App{}
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
			go initiateProducer(r, app, &wg, quit)
		case "consumer":
			app.RegisterConsumer(r)
			go initiateConsumers(r, app, &wg)
		}
	}

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)
	<-termChan
	quit <- true
	wg.Wait()
}

func initiateProducer(ru pkg.Routine, app *api.App, wg *sync.WaitGroup, quit chan bool) {
	defer wg.Done()
	errCounter := 1
	for {
		select {
		case <-quit:
			log.Warnf("sigterm received, safely stopping producer '%s'", ru.Name)
			return
		default:
			err := app.Push(&ru)
			if err != nil {
				if _, ok := err.(*api.MessageFormatError); ok || errCounter > 3 {
					log.Errorf(err.Error())
					log.Warnf("producer '%s' is shutting down due to fatal error.", ru.Name)
					return
				}
				log.Errorf(err.Error())
				errCounter++
				continue
			}
			time.Sleep(time.Duration(ru.Message.Freq) * time.Millisecond)
		}
	}
}

func initiateConsumers(r pkg.Routine, app *api.App, wg *sync.WaitGroup)  {
	defer wg.Done()
	exits, err := app.Rdb.GrpExits(r.Q, r.Group)
	if err != nil {
		log.Errorf("stream '%s' doesn't exits - error : %s", r.Q, err.Error())
		return
	}
	if !exits {
		err = app.Rdb.CreateGrp(r.Q, r.Group)
		if err != nil {
			log.Errorf("failed to create group '%s' - error : %s", r.Group, err.Error())
			return
		}
	}
}