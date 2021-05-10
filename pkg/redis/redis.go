package redis

import (
	"async-comm/pkg/asynccomm/logger"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net"
	"strconv"
	"time"
)

const (
	DefaultConsumerGrpSuf = "-consumer-group"
)

// Redis provides methods to interact with redis database
type Redis struct {
	Ctx    context.Context
	RdbCon *redis.Conn
	Log    logger.Logger
}

// Packet defines the redis consumed messages
type Packet struct {
	Message map[string]interface{}
	Id string
}

type Options struct {
	Network            string
	Addr               string
	Dialer             func(ctx context.Context, network string, addr string) (net.Conn, error)
	OnConnect          func(ctx context.Context, cn *redis.Conn) error
	Username           string
	Password           string
	DB                 int
	MaxRetries         int
	MinRetryBackoff    time.Duration
	MaxRetryBackoff    time.Duration
	DialTimeout        time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
	readOnly           bool
	TLSConfig          *tls.Config
	Limiter            redis.Limiter
	LogLevel           string
	LogFilePath		   string
}

// NewRdb initializes a new Redis instance and establishes a
// connection with the redis server, it also manages the ctx
// at a higher level to be used by all methods of Redis
func NewRdb(ctx context.Context, opts Options) *Redis {
	if opts.LogLevel == "" {
		opts.LogLevel = "panic"
	}
	rdbOpts := &redis.Options{
		Addr: opts.Addr,
		PoolSize: opts.PoolSize,
		ReadTimeout: 5 * time.Second,
		MaxRetries: 3,
		WriteTimeout: 5 * time.Second,
		MinIdleConns: 3,
	}

	log, _ := logger.InitializeLogger(opts.LogLevel, opts.LogFilePath)
	rdbOpts.OnConnect = onConnect
	rdb := redis.NewClient(rdbOpts)
	return &Redis{
		RdbCon: rdb.Conn(ctx),
		Ctx: ctx,
		Log: log,
	}
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
func (r *Redis) Consume(q, name string, block time.Duration) ([]byte, string, error) {
	if block == 0 {
		block = 500 * time.Millisecond
	}
	grp := q + DefaultConsumerGrpSuf
	args := redis.XReadGroupArgs{
		Group:     grp,
		Consumer:  name,
		Streams:   []string{q, ">"},
		Count:     1,
		NoAck:     false,
		Block:     block,
	}
	cmd := r.RdbCon.XReadGroup(r.Ctx, &args)
	res, err := cmd.Result()
	if err != nil {
		return nil, "", err
	}
	r.Log.Debugf("message consumed from stream '%s' by consumer '%s'.'%s'", q, name, grp)
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
func (r *Redis) Ack(q string, msgId... string) error {
	grp := q + DefaultConsumerGrpSuf
	xack := r.RdbCon.XAck(r.Ctx, q, grp, msgId...)
	res, err := xack.Result()
	if err != nil {
		r.Log.Errorf("failed acknowledge { ids: %v, err: %s }", msgId, err.Error())
		return err
	}
	cmd := r.RdbCon.IncrBy(r.Ctx, fmt.Sprintf("%s::%s::acknowledge", q, grp), int64(len(msgId)))
	_, err = cmd.Result()
	if err != nil {
		r.Log.Debugf("failed to increment acknowledge counter { q: %s, grp: %s, error: %s }", q, grp, err.Error())
	}
	r.Log.Debugf("message acknowledge successfully for message %v - res : %d", msgId, res)
	return nil
}

// GrpExists verifies if provided group Exists or not
// it also verifies that the stream we are connecting
// exists if not it will return an error
func (r *Redis) GrpExists(q string) (bool, error) {
	grp := q + DefaultConsumerGrpSuf
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
func (r *Redis) CreateGrp(q string, persistent bool, start string) error {
	if persistent {
		return fmt.Errorf("fatal error! persistence not supported")
	}
	if start == "" {
		start = "$"
	}
	grp := q + DefaultConsumerGrpSuf
	exists, err := r.GrpExists(q)
	if err != nil || !exists {
		cmd := r.RdbCon.XGroupCreateMkStream(r.Ctx, q, grp,  start)
		sts, err := cmd.Result()
		if err != nil {
			return err
		}
		r.Log.Infof("group '%s' for stream '%s' created: %s", grp, q, sts)
	}
	return nil
}

// DeleteGrp creates a new group for provided stream
func (r *Redis) DeleteGrp(q string) error {
	grp := q + DefaultConsumerGrpSuf
	cmd := r.RdbCon.XGroupDestroy(r.Ctx, q, grp)
	sts, err := cmd.Result()
	if err != nil {
		return err
	}
	r.Log.Infof("group '%s' for stream '%s' destroyed: %d", grp, q, sts)
	return nil
}

// StreamExists checks if a stream with given name Exists or not
func (r *Redis) StreamExists(q string) bool {
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
	grp := q + DefaultConsumerGrpSuf
	cmd := r.RdbCon.Del(r.Ctx, q)
	sts, err := cmd.Result()
	if err != nil {
		return err
	}
	r.Log.Infof("stream '%s' deleted: %d", q, sts)
	dCmd := r.RdbCon.Del(r.Ctx, fmt.Sprintf("%s::%s::acknowledge", q, grp))
	_, err = dCmd.Result()
	if err != nil {
		r.Log.Debugf("failed to delete counter %s: %s", fmt.Sprintf("%s::%s::acknowledge", q, grp), err.Error())
	}
	return nil
}

func (r *Redis) DeleteQ(q string) error {
	grp := q + DefaultConsumerGrpSuf
	err := r.DeleteGrp(q)
	if err != nil {
		r.Log.Debugf("deleteGrp failed! { q: %s, grp: %s, err: %s}", q, grp, err.Error())
		return err
	}
	err = r.DeleteStream(q)
	if err != nil {
		r.Log.Debugf("deleteStream failed! { q: %s, grp: %s, err: %s}", q, grp, err.Error())
		return err
	}
	return nil
}

func (r *Redis) PendingStreamMessages(q string, idleTime time.Duration) (map[string][]string, int, error) {
	grp := q + DefaultConsumerGrpSuf
	args := redis.XPendingExtArgs{
		Stream:   q,
		Group:    grp,
		Start:    "-",
		End:      "+",
		Count:    100,
	}
	cmd := r.RdbCon.XPendingExt(context.TODO(), &args)
	pending, err := cmd.Result()
	if err != nil {
		return nil, 0, err
	}
	res := make(map[string][]string)
	for _, mgs := range pending {
		res[mgs.Consumer] = append(res[mgs.Consumer], mgs.ID)
	}
	msgCount := 0
	for _, v := range res {
		msgCount += len(v)
	}
	return res, msgCount, nil
}

func (r *Redis) ClaimPendingMessages(q, consumer string, msgIdleTime time.Duration) error  {
	msgs, _, err := r.PendingStreamMessages(q, msgIdleTime)
	if err != nil {
		r.Log.Errorf("failed to fetch pending messages { q: %s, consumer: %s, error: %s}", q, consumer, err.Error())
		return err
	}
	var errors []error
	for k, ids := range msgs {
		if _, err := r.Get("active_consumers_" + k); err != nil && err.Error() == "redis: nil" {
			if len(ids) > 0 {
				err := r.claimMessages(q, consumer, ids[0])
				if err != nil {
					errors = append(errors, fmt.Errorf("claimed failed! { inActiveConsumer: %s, err : %s }", k, err.Error()))
				}
				r.Log.Debugf("messages claimed! { inActiveConsumer: %s, newConsumer: %s, msgId: %s }", k, consumer, ids[0])
				// returning after one successful claim by the current consumer
				return nil
			}
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("%+v", errors)
	}
	return nil
}

func (r *Redis) GetQStats(opts QStatusOptions) (QStatus, error) {
	status := QStatus{}
	var err error
	status.Info, err = r.getStreamInfo(opts.Q)
	if err != nil {
		return status, err
	}
	if opts.Groups {
		status.Groups = r.getGroupInfo(opts.Q)
	}
	if opts.Consumer {
		status.Consumers = r.getConsumerInfo(opts.Q)
	}
	return status, nil
}

func (r *Redis) getAckMessagesInQ(q string) int64 {
	grp := q + DefaultConsumerGrpSuf
	key := fmt.Sprintf("%s::%s::acknowledge", q, grp)
	cnt, err := r.Get(key)
	if err != nil {
		r.Log.Debugf("failed to fetch total count from increment key '%s': %s",key, err.Error())
		return 0
	}
	acked, _ := strconv.Atoi(cnt)
	return int64(acked)
}

func (r *Redis) getConsumerInfo(q string) []Consumer {
	grp := q + DefaultConsumerGrpSuf
	cmd := r.RdbCon.XInfoConsumers(r.Ctx, q, grp)
	info, err := cmd.Result()
	if err != nil {
		r.Log.Debugf("failed to fetch consumer info '%s': %s", q, err.Error())
		return nil
	}
	var consumerInfo []Consumer
	for _, v := range info {
		consumerInfo = append(consumerInfo, Consumer(v))
	}
	return consumerInfo
}

func (r *Redis) getGroupInfo(q string) []Group {
	cmd := r.RdbCon.XInfoGroups(r.Ctx, q)
	info, err := cmd.Result()
	if err != nil {
		r.Log.Debugf("failed to fetch groups info '%s': %s", q, err.Error())
		return nil
	}
	var grpInfo []Group
	for _, v := range info {
		grpInfo = append(grpInfo, Group(v))
	}
	return grpInfo
}

func (r *Redis) getStreamInfo(q string) (Info, error) {
	cmd := r.RdbCon.XInfoStream(r.Ctx, q)
	info, err := cmd.Result()
	if err != nil {
		r.Log.Debugf("failed to fetch stream info '%s': %s", q, err.Error())
		return Info{}, err
	}
	ackMsgCount := r.getAckMessagesInQ(q)
	return Info{
		Length:          info.Length,
		Acknowledged:    ackMsgCount,
		RadixTreeKeys:   info.RadixTreeKeys,
		RadixTreeNodes:  info.RadixTreeNodes,
		LastGeneratedID: info.LastGeneratedID,
		Groups:          info.Groups,
		FirstEntry:      Message(info.FirstEntry),
		LastEntry:       Message(info.LastEntry),
	}, nil
}

func (r *Redis) claimMessages(q,  consumer string, msgId... string) error {
	grp := q + DefaultConsumerGrpSuf
	args := &redis.XClaimArgs{
		Stream:    q,
		Group:     grp,
		Consumer:  consumer,
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