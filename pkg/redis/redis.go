package redis

import (
	"async-comm/pkg/asynccomm/ac_logger"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/imdario/mergo"
	"reflect"
	"time"
)

// Redis provides methods to interact with redis database
type Redis struct {
	Ctx    context.Context
	RdbCon *redis.Conn
	Log    ac_logger.Logger
}

// Packet defines the redis consumed messages
type Packet struct {
	Message map[string]interface{}
	Id string
}

// NewRdb initializes a new Redis instance and establishes a
// connection with the redis server, it also manages the ctx
// at a higher level to be used by all methods of Redis
func NewRdbWithOpts(ctx context.Context, opts interface{}, log ac_logger.Logger) *Redis {
	rdbOpts := &redis.Options{
		Addr: ":6379",
		PoolSize: 100,
		ReadTimeout: 5 * time.Second,
		MaxRetries: 3,
		WriteTimeout: 5 * time.Second,
		MinIdleConns: 3,
	}
	switch opts.(type) {
	case redis.Options:
		val := opts.(redis.Options)
		mergo.Merge(rdbOpts, val)
	case *redis.Options:
		mergo.Merge(rdbOpts, opts.(*redis.Options))
	default:
		panic(fmt.Sprintf("type redis.Options required provided: %s", reflect.TypeOf(opts)))
	}
	rdbOpts.OnConnect = onConnect
	rdb := redis.NewClient(rdbOpts)
	return &Redis{
		RdbCon: rdb.Conn(ctx),
		Ctx: ctx,
		Log: log,
	}
}

func NewRdb(ctx context.Context, host, port, usr, pwd string, log ac_logger.Logger) *Redis {
	opts := redis.Options{ Addr: fmt.Sprintf("%s:%s", host, port), Username: usr, Password: pwd }
	return NewRdbWithOpts(ctx, opts, log)
}

// Close closes the redis connection when called
func (r *Redis) Close()  {
	r.RdbCon.Close()
}

// Set will provide a way to set a key in redis
func (r *Redis) Set(key, value string, expiry time.Duration) (string, error) {
	cmd := r.RdbCon.Set(r.Ctx, key, value, expiry)
	return cmd.Result()
}

// Get will get the messages with provided key
func (r *Redis) Get(key string) (string, error) {
	cmd := r.RdbCon.Get(r.Ctx, key)
	return cmd.Result()
}

// Produce will push the message to stream for the consumers
func (r *Redis) Produce(qName string, msg string) (string, error) {
	args := &redis.XAddArgs{
		Stream:       qName,
		Values:       map[string]string{ "message": msg },
		ID: 		  "*",
	}
	xadd := r.RdbCon.XAdd(r.Ctx, args)
	res, err := xadd.Result()
	if err != nil {
		return "", err
	}
	r.Log.Debugf("message pushed to stream %s - res : %s", msg, res)
	return res, nil
}

// Consume runs in a infinite loop to check for incoming
// messages on a particular stream, this consumer start
// consuming the latest message which are available to it
// and after it's done consuming it should ack the message
// by calling Ack(). It also take cares of verifying the
// alive consumers and rescheduling of pending messages
// using r.syncConsumers in every RsyncTime interval
func (r *Redis) Consume(ctx context.Context, q, g, name string) ([]byte, string, error) {

	args := redis.XReadGroupArgs{
		Group:     g,
		Consumer:  name,
		Streams:   []string{q, ">"},
		Count:     1,
		NoAck:     false,
	}
	cmd := r.RdbCon.XReadGroup(ctx, &args)
	res, err := cmd.Result()
	if err != nil {
		return nil, "", err
	}
	r.Log.Debugf("message consumed from stream '%s' by consumer '%s'.'%s' : %#v", q, name, g, res)
	for _, xmsg := range res {
		for _, m := range xmsg.Messages {
			return []byte(fmt.Sprintf("%v", m.Values["message"])), m.ID, nil
		}
	}
	return nil, "", fmt.Errorf("redis returned empty messages - %+v", res)
}

// Ack used to acknowledge a message upon successful
// consumption if the message is not ack it will go
// into pending state where it will be reschedule to
// other consumers through syncConsumers
func (r *Redis) Ack(q, g string, msgId... string) error {
	xack := r.RdbCon.XAck(r.Ctx, q, g, msgId...)
	res, err := xack.Result()
	if err != nil {
		r.Log.Errorf("failed acknowledge { ids: %v, err: %s }", msgId, err.Error())
		return err
	}
	r.Log.Debugf("message acknowledge successfully for message %v - res : %d", msgId, res)
	return nil
}

// syncConsumers fetches all the pending messages in
// the stream and check if there consumer is alive
// by verifying it with the sync queue. if the consumer
// is not present in the queue a request will be raised
// by this consumer to claim that message using XClaim
func (r *Redis) syncConsumers() {
	
}

// GrpExits verifies if provided group exits or not
// it also verifies that the stream we are connecting
// exits if not it will return an error
func (r *Redis) GrpExits(q, grp string) (bool, error) {
	cmd := r.RdbCon.XInfoGroups(r.Ctx, q)
	info, err := cmd.Result()
	if err != nil {
		return false, err
	}
	for _, v := range info {
		if v.Name == grp {
			return true, nil
		}
	}
	return false, nil
}

// CreateGrp creates a new group for provided stream
func (r *Redis) CreateGrp(q, grp, start string) error {
	cmd := r.RdbCon.XGroupCreate(r.Ctx, q, grp, start)
	sts, err := cmd.Result()
	if err != nil {
		return err
	}
	r.Log.Infof("group '%s' for stream '%s' created: %s", grp, q, sts)
	return nil
}

// DeleteGrp creates a new group for provided stream
func (r *Redis) DeleteGrp(q, grp string) error {
	cmd := r.RdbCon.XGroupDestroy(r.Ctx, q, grp)
	sts, err := cmd.Result()
	if err != nil {
		return err
	}
	r.Log.Infof("group '%s' for stream '%s' destroyed: %d", grp, q, sts)
	return nil
}

// StreamExits checks if a stream with given name exits or not
func (r *Redis) StreamExits(q string) bool {
	cmd := r.RdbCon.ScanType(r.Ctx, 0, "", 0, "stream")
	sts, _,  err := cmd.Result()
	if err != nil {
		r.Log.Errorf("failed to get stream '%s', error : %s", q, err.Error())
		return false
	}
	for _, v := range sts {
		if v == q {
			return true
		}
	}
	return false
}

// DeleteStream will delete the whole stream with it's data
func (r *Redis) DeleteStream(q string) error {
	cmd := r.RdbCon.Del(r.Ctx, q)
	sts, err := cmd.Result()
	if err != nil {
		return err
	}
	r.Log.Infof("stream '%s' deleted: %d", q, sts)
	return nil
}

func (r *Redis) PendingStreamMessages(q, grp string) (map[string][]string, error) {
	args := &redis.XPendingExtArgs{
		Stream:   q,
		Group:    grp,
		Start: 	  "-",
		End:      "+",
		Count: 	  100,
	}
	cmd := r.RdbCon.XPendingExt(r.Ctx, args)
	pending, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	res := make(map[string][]string)
	for _, mgs := range pending {
		res[mgs.Consumer] = append(res[mgs.Consumer], mgs.ID)
	}
	return res, nil
}

func (r *Redis) ClaimMessages(q, grp,  consumer string, msgId... string) error {
	args := &redis.XClaimArgs{
		Stream:    q,
		Group:     grp,
		Consumer:  consumer,
		MinIdle:   20000,
		Messages:  msgId,
	}
	cmd := r.RdbCon.XClaim(r.Ctx, args)
	msg, err := cmd.Result()
	if err != nil {
		return err
	}
	r.Log.Debugf("message claimed - %#v", msg)
	return nil
}

// onConnect informs if the connection is successfully established
func onConnect(ctx context.Context, conn *redis.Conn) error {
	init := conn.ClientID(ctx)
	id, err := init.Result()
	if err != nil {
		panic(fmt.Sprintf("redis connection failed - please verify your connection string %s \nerror: %s", conn.String(), err.Error()))
	}
	fmt.Println("redis connection established clientId: ", id)
	return nil
}