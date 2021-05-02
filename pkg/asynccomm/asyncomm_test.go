package asynccomm_test

import (
	"asynccomm-lib/asynccomm"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	Q = "async_test_q"
	Consumer = "test_consumerX"
	NewConsumer = "test_consumerY"
	LogLevel = "error"
)

var (
	ac *asynccomm.AsyncComm
	ids []string
)


func TestNewAC(t *testing.T) {
	var err error
	opts := asynccomm.AcOptions{}
	ac, err = asynccomm.NewAC(context.TODO(), opts)
	assert.Nil(t, err)
	assert.NotNil(t, ac)
}

func TestAsyncComm_SetLogLevel(t *testing.T) {
	err := ac.SetLogLevel(LogLevel)
	assert.Nil(t, err)
}

func TestAsyncComm_ConsumeMessageFailure(t *testing.T) {
	_, _, err := ac.Pull(context.TODO(), Q, Consumer)
	assert.NotNil(t, err)
}

func TestAsyncComm_CreateQ(t *testing.T) {
	err := ac.CreateQ(Q)
	assert.Nil(t, err)
}

func TestAsyncComm_Push(t *testing.T) {
	for i:=0; i < 5; i++ {
		t.Run(fmt.Sprintf("Sending_Message_%d", i), func(t *testing.T) {
			msg := []byte(fmt.Sprintf("TestMessage_%s_%d_%s", Q, i, time.Now().String()))
			str, err := ac.Push(Q, msg)
			assert.Nil(t, err)
			t.Logf("message %d pushed successfully - %s", i, str)
		})
	}
}

func TestAsyncComm_Pull(t *testing.T) {
	for i:=0; i < 5; i++ {
		t.Run(fmt.Sprintf("Consuming_Message_%d", i), func(t *testing.T) {
			c := fmt.Sprintf("%s_%d", Consumer, i%2)
			msg, id, err := ac.Pull(context.TODO(), Q, c)
			assert.Nil(t, err)
			t.Logf("message consumed Id: %s, data : %s", id, string(msg))
			ids = append(ids, id)
		})
	}
}

func TestAsyncComm_ClaimPendingMessages(t *testing.T) {
	err := ac.ClaimPendingMessages(Q, NewConsumer)
	assert.Nil(t, err)
}

func TestAsyncComm_Ack(t *testing.T) {
	err := ac.Ack(Q, ids...)
	assert.Nil(t, err)
}

func TestAsyncComm_Pending(t *testing.T)  {
	msgs, err := ac.PendingMessages(Q)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(msgs), msgs)
}

func TestAsyncComm_DeleteQ(t *testing.T) {
	err := ac.DeleteQ(Q)
	assert.Nil(t, err)
}

func TestAsyncComm_GroupExits(t *testing.T) {
	exits := ac.GroupExits(Q)
	assert.Equal(t, false, exits)
}
