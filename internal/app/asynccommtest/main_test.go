package main_test

import (
	"async-comm/internal/app/asynccommtest"
	"async-comm/pkg/asynccomm"
	acRedis "async-comm/pkg/redis"
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

const (
	LogLevel = "info"
	Q = "app_test_q"
)

var (
	a *main.App
)

var ProducerRoutine = main.Routine{
	Role:  "producer",
	Q:     Q,
	Name:  "app_test_producer",
	Message: struct {
    	FormattedMsg string `json:"formatted_msg" mapstructure:"formatted_msg"`
    	Format       string `json:"format" mapstructure:"format"`
    	Freq         int    `json:"freq" mapstructure:"freq"`
	}{
		Format: "{{.App}}-{{.Producer}}-{{.Time}}",
		Freq: 500,
	},
}

var ConsumerRoutine = main.Routine{
	Role:  "consumer",
	Q:      Q,
	Name:  "app_test_consumer",
	RefreshTime: 10000,
	ProcessingTime: 100,
}

func TestMain(m *testing.M) {
	rdOpts := acRedis.Options{}
	rdb := acRedis.NewRdb(context.TODO(), rdOpts)
	aclib, _ := asynccomm.NewAC(rdb)
	cnf := main.InitializeConfig()
	cnf.Logger.Level = LogLevel
	a = main.NewApp(aclib, cnf, main.InitializeLogger(cnf))
	m.Run()
}

func TestInitializeProducer(t *testing.T)  {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go a.InitiateProducer(ctx, ProducerRoutine, "TestCase", wg)
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
}

func TestInitializeConsumer(t *testing.T)  {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go a.InitiateConsumers(ctx, ConsumerRoutine, wg)
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestDeleteQ(t *testing.T)  {
	rdbOpts := acRedis.Options{}
	rdb := acRedis.NewRdb(context.TODO(), rdbOpts)
	aclib, _ := asynccomm.NewAC(rdb)
	cnf := main.InitializeConfig()
	cnf.Logger.Level = LogLevel
	a = main.NewApp(aclib, cnf, main.InitializeLogger(cnf))
	err := a.DeleteQ(Q)
	assert.Nil(t, err)
}