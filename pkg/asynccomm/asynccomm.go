package asynccomm

import (
	"async-comm/pkg/asynccomm/logger"
	"async-comm/pkg/redis"
	"context"
	"os"
	"sync"
	"time"
)

const DefaultConsumerGroup = "-consumer-group"

type AsyncComm struct {
	Rdb *redis.Redis
	Log logger.Logger
	startTimes map[string]time.Time
}

// NewAC creates a asyncComm library instance for managing
// async message methods like push, pull etc
func NewAC(rdb *redis.Redis) (*AsyncComm, error) {
	return &AsyncComm{Rdb: rdb, Log: rdb.Log, startTimes: make(map[string]time.Time)}, nil
}

// SetLogLevel provides different log levels for the async library
// by default it's set to error, available log levels are debug,
// info, error, panic
func (ac *AsyncComm) SetLogLevel(level string) error {
	log, err := logger.SetLevel(level)
	if err != nil {
		return err
	}
	ac.Log = log
	ac.Rdb.Log = log
	return nil
}

func (ac *AsyncComm) Push(q string, msg []byte) (string, error) {
	return ac.Rdb.Produce(q, string(msg))
}

func (ac *AsyncComm) Pull(ctx context.Context, q, consumer string, refreshTime int) ([]byte, string, error) {
	if time.Now().After(ac.getStartTime(consumer, refreshTime)) {
		err := ac.ClaimPendingMessages(q, consumer)
		if err != nil {
			ac.Log.Errorf("failed to claim pending messages for stream '%s' by consumer '%s': %s", q, consumer, err.Error())
		}
		ac.Log.Debugf("pending message claim request initiated by consumer '%s'", consumer)
		ac.setStartTime(consumer, refreshTime)
	}
	return ac.Rdb.Consume(ctx, q, consumer)
}

func (ac *AsyncComm) Ack(q string, msgId... string) error {
	return ac.Rdb.Ack(q, msgId...)
}

func (ac *AsyncComm) CreateQ(q string, persistent bool) error {
	return ac.Rdb.CreateGrp(q, persistent, "")
}

func (ac *AsyncComm) ClaimPendingMessages(q, consumer string) error {
	return ac.Rdb.ClaimPendingMessages(q, consumer)
}

func (ac *AsyncComm) DeleteQ(q string) error {
	return ac.Rdb.DeleteQ(q)
}

func (ac *AsyncComm) FlushQ(q string) error {
	return ac.Rdb.DeleteStream(q)
}

func (ac *AsyncComm) PendingMessages(q string) (map[string][]string, int, error) {
	return ac.Rdb.PendingStreamMessages(q)
}

func (ac *AsyncComm) GroupExists(q string) bool {
	exists, err := ac.Rdb.GrpExists(q)
	if err != nil {
		ac.Log.Debugf("encountered error verifying group ! { q: %s, grp: %s, err: %s}", q, q + DefaultConsumerGroup, err.Error())
		return false
	}
	return exists
}

func (ac *AsyncComm) getStartTime(consumer string, refreshTime int) time.Time {
	if val, ok := ac.startTimes[consumer]; ok {
		return val
	}
	return ac.setStartTime(consumer, refreshTime)
}

func (ac *AsyncComm) setStartTime(consumer string, refreshTime int) time.Time {
	now := time.Now().Add(time.Duration(refreshTime) * time.Millisecond)
	ac.startTimes[consumer] = now
	return now
}

func (ac *AsyncComm) RegisterConsumer(ctx context.Context, cnsmr string, rTime int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if cnsmr == os.Getenv("TEST_CONSUMER") {
				return
			}
			sts, err := ac.Rdb.Set("active_consumer_" + cnsmr, time.Now().String(), time.Duration(rTime) * time.Millisecond)
			if err != nil {
				ac.Log.Panicf("failed to register consumer %s - %s", cnsmr, err.Error())
			}
			refreshTime := rTime - (rTime/10)
			ac.Log.Infof("consumer '%s' registered with refreshTime: '%dms', status: '%s'", cnsmr, refreshTime, sts)
			time.Sleep(time.Duration(refreshTime) * time.Millisecond)
		}
	}
}

func (ac *AsyncComm) Close()  {
	ac.Rdb.Close()
}