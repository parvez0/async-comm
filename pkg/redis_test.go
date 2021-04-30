package pkg_test

import (
	"async-comm/pkg"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var cnf *pkg.Config
var rdb *pkg.Redis

var Q = "test_queue"
var Group = "test_group1"
var Consumers = []string{"test_consumer1", "test_consumer2"}

func Test_RedisConnection(t *testing.T) {
	cnf = pkg.InitializeConfig()
	pkg.InitializeLogger()
	rdb = pkg.NewRdb(context.TODO(), cnf.Redis, 0)
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
	err := rdb.CreateGrp(Q, Group)
	assert.Nil(t, err)
}

func Test_RedisConsumer(t *testing.T) {
	for i := 0; i < 5; i++ {
		wrk := Consumers[i%2]
		t.Run(fmt.Sprintf("%s_%d", wrk, i), func(t *testing.T) {
			r := pkg.Routine{
				Role:  "consumer",
				Q:     Q,
				Name:  wrk,
				Group: Group,
				ProcessingTime: 100,
				RefreshTime:    1000,
			}
			res, err := rdb.Consume(r)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("recieved response from queue %+v", res)
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