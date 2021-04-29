package pkg

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

// Redis provides methods to interact with redis database
type Redis struct {
	RdbCon    *redis.Conn
	Ctx       context.Context
	RsyncTime time.Time
}

// Packet defines the redis consumed messages
type Packet struct {
	Message map[string]interface{}
	Id string
}

// NewRdb initializes a new Redis instance and establishes a
// connection with the redis server, it also manages the ctx
// at a higher level to be used by all methods of Redis
func NewRdb(ctx context.Context, rCnf RedisConf, rsyncInterval int) *Redis {
	// setting initial rsync time for clients
	if rsyncInterval == 0 {
		rsyncInterval = 5000
	}
	// Options contains all the information about setting up a
	// persistent connection with the redis server.
	redisOpts := redis.Options{
		Addr:               fmt.Sprintf("%s:%s", rCnf.Host, rCnf.Port),
		OnConnect:          onConnect,
		MaxRetries:         5,
		MinRetryBackoff:    250 * time.Millisecond,
		MaxRetryBackoff:    1 * time.Second,
		PoolSize:           10,
		MinIdleConns:       3,
		IdleTimeout:        2 * time.Minute,
	}
	rdb := redis.NewClient(&redisOpts)
	return &Redis{
		RdbCon: rdb.Conn(ctx),
		Ctx: ctx,
		RsyncTime: time.Now().Add(time.Duration(rsyncInterval) * time.Millisecond),
	}
}

// Close closes the redis connection when called
func (r *Redis) Close()  {
	r.RdbCon.Close()
}

// Set will provide a way to set a key in redis
func (r *Redis) Set(key, value string, expiry time.Duration) (string, error) {
	status := r.RdbCon.Set(r.Ctx, key, value, expiry)
	return status.Result()
}

// Produce will push the message to stream for the consumers
func (r *Redis) Produce(qName, msg string) error {
	args := &redis.XAddArgs{
		Stream:       qName,
		Values:       msg,
	}
	xadd := r.RdbCon.XAdd(r.Ctx, args)
	res, err := xadd.Result()
	if err != nil {
		return err
	}
	log.Debugf("message pushed to stream %s - res : %s", msg, res)
	return nil
}

// Consume runs in a infinite loop to check for incoming
// messages on a particular stream, this consumer start
// consuming the latest message which are available to it
// and after it's done consuming it should ack the message
// by calling Ack(). It also take cares of verifying the
// alive consumers and rescheduling of pending messages
// using r.syncConsumers in every RsyncTime interval
func (r *Redis) Consume(ch chan Packet, ru Routine) {
	args := redis.XReadGroupArgs{
		Group:     ru.Group,
		Consumer:  ru.Name,
		Streams:   []string{ru.Q, ">"},
		Count:     1,
		NoAck:     false,
	}
	for {
		if time.Now().After(r.RsyncTime) {
			r.syncConsumers()
			r.RsyncTime = time.Now().Add(time.Duration(ru.RefreshTime) * time.Millisecond)
		}
		cmd := r.RdbCon.XReadGroup(r.Ctx, &args)
		res, err := cmd.Result()
		if err != nil {
			log.Errorf("failed to read messages - %s, trying again", err.Error())
			continue
		}
		log.Debug("message received from redis - ", res)
		time.Sleep(time.Duration(ru.ProcessingTime) * time.Millisecond)
		for _, v := range res {
			for _, msg := range v.Messages {
				pkt := Packet{
					Message: msg.Values,
					Id:      msg.ID,
				}
				ch <- pkt
			}
		}
	}
}

// Ack used to acknowledge a message upon successful
// consumption if the message is not ack it will go
// into pending state where it will be reschedule to
// other consumers through syncConsumers
func (r *Redis) Ack(ru Routine, retry int, msgId... string) error {
	xack := r.RdbCon.XAck(r.Ctx, ru.Q, ru.Group, msgId...)
	res, err := xack.Result()
	if err != nil {
		log.Debugf("failed acknowledge : %v - %s", msgId, err.Error())
		if retry < 3 {
			return r.Ack(ru, retry + 1, msgId...)
		}
		log.Debugf("too many ack retries for messages - %v", msgId)
		return err
	}
	log.Debugf("message acknowledge successfully for message %v - res : %d", msgId, res)
	return nil
}

// syncConsumers fetches all the pending messages in
// the stream and check if there consumer is alive
// by verifying it with the sync queue. if the consumer
// is not present in the queue a request will be raised
// by this consumer to claim that message using XClaim
func (r *Redis) syncConsumers() {
	
}

// onConnect inform us if the connection is successfully established
func onConnect(ctx context.Context, conn *redis.Conn) error {
	init := conn.ClientID(ctx)
	id, err := init.Result()
	if err != nil {
		log.Panicf("redis connection failed - please verify your connection string %s \nerror: %s", conn.String(), err.Error())
	}
	log.Info("redis connection established clientId: ", id)
	return nil
}