package redis_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"kiran-anand14/async-comm/pkg/asynccomm/ac_logger"
	"kiran-anand14/async-comm/pkg/redis"
	"testing"
	"time"
)

const (
	Q = "test_queue"
	Group = "test_group1"
	RedisHost = ""
	RedisPort = "6379"
	LogLevel = "debug"
)

var (
	rdb *redis.Redis
	Consumers = []string{"test_consumer1", "test_consumer2"}
)

func Test_RedisConnection(t *testing.T) {
	log, _ := ac_logger.InitializeLogger(LogLevel, "")
	rdb = redis.NewRdb(context.TODO(), RedisHost, RedisPort,"", "", log)
}

func Test_RedisProducer(t *testing.T)  {
	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("test_message_%d_%s", i, time.Now().String())
		t.Run(msg, func(t *testing.T) {
			_, err := rdb.Produce(Q, msg)
			assert.Nil(t, err)
		})
	}
}

func Test_RedisCheckIfGroupExits(t *testing.T)  {
	res, err := rdb.GrpExits(Q, Group)
	assert.Nil(t, err, "group verification failed with error - ", err)
	assert.Equal(t, res, false)
}

func Test_RedisCreateGroup(t *testing.T)  {
	err := rdb.CreateGrp(Q, Group, "0")
	assert.Nil(t, err)
}

func Test_RedisConsumer(t *testing.T) {
	for i := 0; i < 5; i++ {
		wrk := Consumers[i%2]
		t.Run(fmt.Sprintf("%s_%d", wrk, i), func(t *testing.T) {
			res, id, err := rdb.Consume(context.TODO(), Q, Group, wrk)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("recieved response from queue { msg: %s, id: %s }", res, id)
		})
	}
}

func Test_RedisDestroyGroup(t *testing.T) {
	err := rdb.DeleteGrp(Q, Group)
	assert.Nil(t, err)
}

func Test_RedisDeleteStream(t *testing.T) {
	err := rdb.DeleteStream(Q)
	assert.Nil(t, err)
}