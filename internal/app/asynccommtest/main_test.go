package main_test

import (
	"async-comm/internal/app/asynccommtest/src"
	"async-comm/pkg/asynccomm"
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
	a *src.App
)

var ProducerRoutine = src.Routine{
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

var ConsumerRoutine = src.Routine{
	Role:  "consumer",
	Q:      Q,
	Name:  "app_test_consumer",
	RefreshTime: 10000,
	ProcessingTime: 100,
}

func TestMain(m *testing.M) {
	aclib, _ := asynccomm.NewAC(context.TODO(), asynccomm.AcOptions{})
	cnf := src.InitializeConfig()
	cnf.Logger.Level = LogLevel
	a = src.NewApp(aclib, cnf, src.InitializeLogger(cnf))
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
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
}

func TestDeleteQ(t *testing.T)  {
	err := a.DeleteQ(Q)
	assert.Nil(t, err)
}