package main

import (
	"async-comm/pkg/asynccomm"
	"async-comm/pkg/redis"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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
	// Reviewed-Changes - initializing redis library and passing it to asynccomm
	rdOpts := redis.Options{
		Addr: fmt.Sprintf("%s:%s", cnf.Redis.Host, cnf.Redis.Port),
		LogLevel: cnf.AcLogger.Level,
		LogFilePath: cnf.AcLogger.OutputFilePath,
	}
	log.Debugf("starting redis with conf: %+v", cnf.Redis)
	cRdb := redis.NewRdb(context.TODO(), rdOpts)
	pRdb := redis.NewRdb(context.TODO(), rdOpts)
	cAclib, err := asynccomm.NewAC(cRdb)
	pAclib, err := asynccomm.NewAC(pRdb)
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
			wg.Add(1)
			// Reviewed changes: registering consumer with keep alive at every r.RefreshTime interval
			cns := asynccomm.Consumer{
				Name:            r.Name,
				ClaimInterval:   time.Duration(r.ClaimTime),
				BlockTime:       time.Duration(r.BlockTime),
				RefreshInterval: time.Duration(r.RefreshTime),
				MsgIdleDuration: time.Duration(r.MsgIdleTime),
			}
			cApp.aclib.RegisterConsumer(ctx, cns)
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
	CloseLogger()
	wg.Wait()
}






