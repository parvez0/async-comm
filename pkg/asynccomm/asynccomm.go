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

const (
	ConsumerClaimInterval=5000
	ConsumerRefreshInterval=5000
	ConsumerBlockTime=300
)

type AsyncComm struct {
	Rdb *redis.Redis
	Log logger.Logger
	consumers []Consumer
}

type Consumer struct {
	Name 		string
	ClaimInterval       time.Duration
	BlockTime       	time.Duration
	RefreshInterval 	time.Duration
	Wg					*sync.WaitGroup
	ctx 				context.Context
	cancel 				context.CancelFunc
	lastSync       		*time.Time
}

var lock = sync.RWMutex{}

// NewAC creates a asyncComm library instance for managing
// async message methods like push, pull etc
func NewAC(rdb *redis.Redis) (*AsyncComm, error) {
	return &AsyncComm{Rdb: rdb, Log: rdb.Log}, nil
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
	if time.Now().After(*tm) {
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
		ac.Log.Debugf("encountered error verifying group ! { q: %s, err: %s}", q, err.Error())
		return false
	}
	return exists
}

func (ac *AsyncComm) RegisterConsumer(c Consumer) error {
	if c.Name == "" {
		return fmt.Errorf("consumer name is required for creating new consumer")
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = ConsumerRefreshInterval
	}
	if c.BlockTime == 0 {
		c.BlockTime = ConsumerBlockTime
	}
	if c.ClaimInterval == 0 {
		c.ClaimInterval = ConsumerClaimInterval
	}
	if c.Wg == nil {
		c.Wg = new(sync.WaitGroup)
	}
	syncTime := time.Now().Add(c.RefreshInterval * time.Millisecond)
	c.lastSync = &syncTime
	c.ctx, c.cancel = context.WithCancel(ac.Rdb.Ctx)
	go ac.syncConsumer(&c)
	lock.Lock()
	defer lock.Unlock()
	ac.consumers = append(ac.consumers, c)
	return nil
}

func (ac *AsyncComm) DeRegisterConsumer(consumer string) error {
	c := ac.getConsumer(consumer)
	if c != nil {
		c.cancel()
		return nil
	}
	return fmt.Errorf("no such registered consumer '%s'", consumer)
}

func (ac *AsyncComm) GetQStats(opts redis.QStatusOptions) (redis.QStatus, error) {
	return ac.Rdb.GetQStats(opts)
}

func (ac *AsyncComm) getStartTime(consumer string) (*time.Time, error) {
	lock.RLock()
	defer lock.RUnlock()
	c := ac.getConsumer(consumer)
	if c != nil {
		return c.lastSync, nil
	}
	return nil, fmt.Errorf("initial claimTime not set for consumer: %s", consumer)
}

func (ac *AsyncComm) setStartTime(consumer string) *time.Time {
	lock.Lock()
	defer lock.Unlock()
	c := ac.getConsumer(consumer)
	if c != nil {
		syncTime := time.Now().Add(c.RefreshInterval * time.Millisecond)
		c.lastSync = &syncTime
		return &syncTime
	}
	syncTime := time.Now().Add(5000 * time.Millisecond)
	return &syncTime
}

func (ac *AsyncComm) getConsumer(consumer string) *Consumer  {
	for _, c := range ac.consumers {
		if c.Name == consumer {
			return &c
		}
	}
	return nil
}

func (ac *AsyncComm) syncConsumer(c *Consumer)  {
	defer c.Wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if c.Name == os.Getenv("TEST_CONSUMER") {
				return
			}
			sts, err := ac.Rdb.Set("active_consumer_" + c.Name, time.Now().String(), c.RefreshInterval * time.Millisecond)
			if err != nil {
				ac.Log.Panicf("failed to register consumer %s - %s", c.Name, err.Error())
			}
			refreshTime := c.RefreshInterval - (c.RefreshInterval/10)
			ac.Log.Infof("consumer '%s' registered with refreshTime: '%dms', status: '%s'", c.Name, refreshTime, sts)
			time.Sleep(time.Duration(refreshTime) * time.Millisecond)
		}
	}
}

func (ac *AsyncComm) Close()  {
	ac.Rdb.Close()
}