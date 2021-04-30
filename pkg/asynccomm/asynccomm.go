package asynccomm

import (
	"async-comm/pkg/asynccomm/ac_logger"
	"async-comm/pkg/redis"
	"context"
	"fmt"
)

const DefaultConsumerGroup = "-consumer-group"

type AcOptions struct {
	Redis *RedisOpts
	Logger *LoggerOpts
}

type RedisOpts struct {
	Host string
	Port string
	Username string
	Password string
}

type LoggerOpts struct {
	Level string
	OutputFilePath string
	FullTimeStamp bool
}

type AsyncComm struct {
	Rdb *redis.Redis
	Log ac_logger.Logger
}

// NewAC creates a asyncComm library instance for managing
// async message methods like push, pull etc
func NewAC(ctx context.Context, opts AcOptions) (*AsyncComm, error) {
	if opts.Logger == nil {
		opts.Logger = &LoggerOpts{
			Level:          "error",
			OutputFilePath: "",
		}
	}
	if opts.Redis == nil {
		opts.Redis = &RedisOpts{
			Port:     "6379",
		}
	}
	log, err := ac_logger.InitializeLogger(opts.Logger.Level, opts.Logger.OutputFilePath)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewRdb(ctx, opts.Redis.Host, opts.Redis.Port, opts.Redis.Username, opts.Redis.Password, log)
	return &AsyncComm{Rdb: rdb, Log: log}, nil
}

// SetLogLevel provides different log levels for the async library
// by default it's set to error, available log levels are debug,
// info, error, panic
func (ac *AsyncComm) SetLogLevel(level string) error {
	log, err := ac_logger.SetLevel(level)
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

func (ac *AsyncComm) Pull(q, consumer string) ([]byte, string, error) {
	return ac.Rdb.Consume(q, q + DefaultConsumerGroup, consumer)
}

func (ac *AsyncComm) Ack(q string, msgId... string) error {
	return ac.Rdb.Ack(q, q + DefaultConsumerGroup, msgId...)
}

func (ac *AsyncComm) CreateQ(q string) error {
	grp := q + DefaultConsumerGroup
	exits := ac.Rdb.StreamExits(q)
	if !exits {
		// code for creating the q should go here
		ac.Rdb.Produce(q, "initial message")
	}
	exits, err := ac.Rdb.GrpExits(q, grp)
	if err != nil {
		ac.Log.Debugf("failed to verify if group exits with err : %s", err.Error())
		return err
	}
	if !exits {
		return ac.Rdb.CreateGrp(q, grp, "$")
	}
	return nil
}

func (ac *AsyncComm) ClaimPendingMessages(q, consumer string) error {
	grp := q + DefaultConsumerGroup
	msgs, err := ac.Rdb.PendingStreamMessages(q, grp)
	if err != nil {
		return err
	}
	var errors []error
	for k, ids := range msgs {
		if _, err := ac.Rdb.Get("active_consumers_" + k); err != nil {
			err := ac.Rdb.ClaimMessages(q, grp, consumer, ids...)
			if err != nil {
				errors = append(errors, fmt.Errorf("claimed failed! { inActiveConsumer: %s, err : %s }", k, err.Error()))
			}
			ac.Log.Debugf("messages claimed! { inActiveConsumer: %s, newConsumer: %s }", k, consumer)
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("%+v", errors)	
	}
	return nil
}

func (ac *AsyncComm) DeleteQ(q string) error {
	err := ac.Rdb.DeleteGrp(q, q + DefaultConsumerGroup)
	if err != nil {
		ac.Log.Debugf("deleteGrp failed! { q: %s, grp: %s, err: %s}", q, q + DefaultConsumerGroup, err.Error())
		return err
	}
	err = ac.Rdb.DeleteStream(q)
	if err != nil {
		ac.Log.Debugf("deleteStream failed! { q: %s, grp: %s, err: %s}", q, q + DefaultConsumerGroup, err.Error())
		return err
	}
	return nil
}

func (ac *AsyncComm) FlushQ(q string) error {
	return ac.Rdb.DeleteStream(q)
}

func (ac *AsyncComm) PendingMessages(q string) (map[string][]string, error) {
	return ac.Rdb.PendingStreamMessages(q, q + DefaultConsumerGroup)
}

func (ac *AsyncComm) GroupExits(q string) bool {
	exits, err := ac.Rdb.GrpExits(q, q + DefaultConsumerGroup)
	if err != nil {
		ac.Log.Debugf("encountered error verifying group ! { q: %s, grp: %s, err: %s}", q, q + DefaultConsumerGroup, err.Error())
		return false
	}
	return exits
}