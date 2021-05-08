package redis_test

import (
	"async-comm/pkg/redis"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
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
	opts := redis.Options{
		LogLevel: LogLevel,
	}
	rdb = redis.NewRdb(context.TODO(), opts)
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

func Test_RedisCheckIfGroupExists(t *testing.T)  {
	res, err := rdb.GrpExists(Q)
	assert.Nil(t, err, "group verification failed with error - ", err)
	assert.Equal(t, res, false)
}

func Test_RedisCreateGroup(t *testing.T)  {
	err := rdb.CreateGrp(Q, false, "0")
	assert.Nil(t, err)
}

func Test_RedisConsumerAndAck(t *testing.T) {
	var ids []string
	t.Run("RedisConsumer", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			wrk := Consumers[i%2]
			t.Run(fmt.Sprintf("%s_%d", wrk, i), func(t *testing.T) {
				res, id, err := rdb.Consume(Q, wrk, 200)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("recieved response from queue { msg: %s, id: %s }", res, id)
				ids = append(ids, id)
			})
		}
	})
	t.Run("AcknowledgeMessages", func(t *testing.T) {
		err := rdb.Ack(Q, ids...)
		assert.Nil(t, err)
	})
}

func TestRedis_GetQStats(t *testing.T) {
	opts := redis.QStatusOptions{
		Q:        Q,
		Consumer: true,
		Groups:   true,
	}
	exp := redis.QStatus{}
	status, err := rdb.GetQStats(opts)
	assert.Nil(t, err)
	t.Logf("stats info : %#v", status)
	assert.NotEqual(t, status, exp)
}

func Test_RedisDestroyGroup(t *testing.T) {
	err := rdb.DeleteGrp(Q)
	assert.Nil(t, err)
}

func Test_RedisDeleteStream(t *testing.T) {
	err := rdb.DeleteStream(Q)
	assert.Nil(t, err)
}