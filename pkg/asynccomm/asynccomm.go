package asynccomm

import (
	"async-comm/pkg/asynccomm/logger"
	"async-comm/pkg/redis"
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

const DefaultConsumerGroup = "-consumer-group"

type AsyncComm struct {
	Rdb *redis.Redis
	Log logger.Logger
	ClaimTime int
	startTimes map[string]time.Time
}

var lock = sync.RWMutex{}

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

func (ac *AsyncComm) Pull(q, consumer string, block time.Duration) ([]byte, string, error) {
	tm, err := ac.getStartTime(consumer)
	if err != nil {
		tm = ac.setStartTime(consumer)
	}
	if time.Now().After(tm) {
		err := ac.ClaimPendingMessages(q, consumer)
		if err != nil {
			ac.Log.Errorf("failed to claim pending messages for stream '%s' by consumer '%s': %s", q, consumer, err.Error())
		}
		ac.Log.Debugf("pending message claim request initiated by consumer '%s'", consumer)
		ac.setStartTime(consumer)
	}
	return ac.Rdb.Consume(q, consumer, block)
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

func (ac *AsyncComm) RegisterConsumer(ctx context.Context, cnsmr string, rTime, claimTime int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ac.ClaimTime = claimTime
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

func (ac *AsyncComm) GetQStats(opts redis.QStatusOptions) (redis.QStatus, error) {
	return ac.Rdb.GetQStats(opts)
}

func (ac *AsyncComm) getStartTime(consumer string) (time.Time, error) {
	lock.RLock()
	defer lock.RUnlock()
	if val, ok := ac.startTimes[consumer]; ok {
		return val, nil
	}
	return time.Time{}, fmt.Errorf("initial claimTime not set for consumer: %s", consumer)
}

func (ac *AsyncComm) setStartTime(consumer string) time.Time {
	if ac.ClaimTime == 0 {
		ac.ClaimTime = 5000
	}
	now := time.Now().Add(time.Duration(ac.ClaimTime) * time.Millisecond)
	lock.Lock()
	defer lock.Unlock()
	ac.startTimes[consumer] = now
	ac.Log.Debugf("refreshed next claimTime : %s", now.String())
	return now
}

func (ac *AsyncComm) Close()  {
	ac.Rdb.Close()
}