package main

import (
	"asynccomm-app/asynccommtest/src"
	"asynccomm-lib/asynccomm"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var cnf *src.Config
var log src.Logger

func init()  {
	cnf = src.InitializeConfig()
	log = src.InitializeLogger(cnf)
}

func main()  {
	/**
	 *  asynccomm library invocation and app initialization
	 */
	rdOpts := asynccomm.RedisOpts(cnf.Redis)
	acLogOpts := asynccomm.LoggerOpts(cnf.AcLogger)
	rdOpts.PoolSize = 100
	acOpts := asynccomm.AcOptions{
		Redis:  &rdOpts,
		Logger: &acLogOpts,
	}
	c_aclib, err := asynccomm.NewAC(context.TODO(), acOpts)
	p_aclib, err := asynccomm.NewAC(context.TODO(), acOpts)
	c_app := src.NewApp(c_aclib, cnf, log)
	p_app := src.NewApp(p_aclib, cnf, log)
	/**
	 * WaitGroups and channels initialization
	 */
	wg := new(sync.WaitGroup)
	ctx, quit := context.WithCancel(context.Background())
	/**
	 *  consumers and producers initialization
	 */
	if err != nil {
		log.Panicf("failed to initialize asynccomm - error : %s", err.Error())
	}
	for _, r := range cnf.App.Routines {
		switch r.Role {
		case "producer":
			wg.Add(1)
			// creating producer routine to send messages at every given interval
			go p_app.InitiateProducer(ctx, r, cnf.App.App, wg)
		case "consumer":
			wg.Add(2)
			go c_app.RegisterConsumer(ctx, r, wg)
			go c_app.InitiateConsumers(ctx, r, wg)
		}
	}
	/**
	 *  application handler logic
	 */
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)
	<-termChan
	quit()
	wg.Wait()
}






