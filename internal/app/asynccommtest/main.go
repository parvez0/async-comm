package main

import (
	"async-comm/pkg/asynccomm"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var cnf *Config

func init()  {
	cnf = InitializeConfig()
	InitializeLogger(cnf)
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
	cAclib, err := asynccomm.NewAC(context.TODO(), acOpts)
	pAclib, err := asynccomm.NewAC(context.TODO(), acOpts)
	cApp := NewApp(cAclib, cnf, log)
	pApp := NewApp(pAclib, cnf, log)
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
			go pApp.InitiateProducer(ctx, r, cnf.App.App, wg)
		case "consumer":
			wg.Add(2)
			// registering consumer with keep alive at every r.RefreshTime interval
			go cApp.aclib.RegisterConsumer(ctx, r.Name, r.RefreshTime, wg)
			go cApp.InitiateConsumers(ctx, r, wg)
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






