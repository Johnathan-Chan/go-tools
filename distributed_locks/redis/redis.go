package redis

import (
	"context"
	"errors"
	"github.com/Johnathan-Chan/go-tools/distributed_locks"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

var (
	GetLockFailedErr = errors.New("redis the lock failed")
	RenewTimeErr     = errors.New("the renewTime must smaller than timeout")
)

// RedisAgent the redis manager that manage the local key
type RedisAgent struct {
	// store the redis key in local, the local value is the *redisLock
	keyMap sync.Map
	// the redis client
	client *redis.Client
	// agent lock that prevent duplicate creation of objects
	lock sync.Mutex
	// manager key
	key string
}

func NewRedisAgent(client *redis.Client) distributed_locks.Agent {
	agent := &RedisAgent{client: client, key: "redis_agent_manager_lock"}

	// create the lock pipeline
	go agent.subscribe()
	return agent
}

// Lock obtain the lock
func (r *RedisAgent) Lock(key string, timeout, renewTime int64) error {
	// the renewTime must smaller than timeout
	if timeout <= renewTime {
		return RenewTimeErr
	}

	// Get the local lock
	lockObj, ok := r.keyMap.Load(key)
	if !ok {
		// prevent duplicate creation of objects
		r.lock.Lock()
		defer r.lock.Unlock()

		lockObj, ok = r.keyMap.Load(key)
		if !ok {
			newContext, cancelFunc := context.WithCancel(r.client.Context())
			lockObj = newRedisLock(r.client, newContext,cancelFunc)
			r.keyMap.Store(key, lockObj)
		}
	}

	// distributed lock to local lock
	redisLockObj := lockObj.(*redisLock)

	ReLock:
	redisLockObj.lock.Lock()

	// get redis lock
	if err := redisLockObj.obtainLock(redisLockObj.ctx, key, timeout, renewTime); err != nil{
		// get redis lock failed, block the process
		// when the lock release, go to reLock
		goto ReLock
	}

	return nil
}

// UnLock release the lock
func (r *RedisAgent) UnLock(key string){
	// Get the local lock
	lockObj, ok := r.keyMap.Load(key)
	if !ok {
		return
	}

	// distributed lock to local lock
	redisLockObj, ok := lockObj.(*redisLock)
	if !ok {
		return
	}

	// cancel the renew
	redisLockObj.cancelFunc()
	// release the redis lock
	if err := redisLockObj.releaseLock(key); err != nil{
		return
	}

	//  publish the lock topic about key
	if err := r.publish(key); err != nil{
		return
	}
}

// publish that publish the lock topic about key
func (r *RedisAgent) publish(key string) error {
	return r.client.Publish(r.client.Context(), r.key, key).Err()
}

// subscribe that subscribe the lock topic
func (r *RedisAgent) subscribe() {
	pipeline := r.client.Subscribe(r.client.Context(), r.key).Channel()
	for {
		select {
		case msg := <-pipeline:
			key := msg.Payload
			// Get the local lock
			lockObj, ok := r.keyMap.Load(key)
			if !ok {
				continue
			}

			// distributed lock to local lock
			redisLockObj, ok := lockObj.(*redisLock)
			if !ok {
				continue
			}

			// release the local lock
			redisLockObj.lock.Unlock()
		case <-r.client.Context().Done():
			return
		}
	}
}

// redisLock redis lock map to the lock mutex
type redisLock struct {
	lock       sync.Mutex
	client     *redis.Client
	ctx        context.Context
	cancelFunc context.CancelFunc
	noFirst      bool
}

func newRedisLock(client *redis.Client, ctx context.Context, cancelFunc context.CancelFunc) *redisLock {
	return &redisLock{client: client, ctx: ctx, cancelFunc: cancelFunc}
}

// obtainLock get the redis lock
func (r *redisLock) obtainLock(ctx context.Context, key string, timeout, renewTime int64) error {
	var (
		result interface{}
		err error
	)

	// if the first, will keep acquiring locks until you know success
	// if no first, will just request once
	for result, err = r.client.Do(r.client.Context(), "SET", key, 1, "NX", "EX", timeout).Result();
		(result != "OK" || err != nil) && !r.noFirst;{

		result, err = r.client.Do(r.client.Context(), "SET", key, 1, "NX", "EX", timeout).Result()
		if result == "OK" && err == nil {
			r.noFirst = true
			break
		}
	}

	if result == "OK" && err == nil {
		// if get the redis lock success it will be go to renew
		go r.renew(ctx, key, timeout, renewTime)
		return nil
	}

	return GetLockFailedErr
}

// releaseLock release the redis lock
func (r *redisLock) releaseLock(key string) error {
	_, err := r.client.Del(r.client.Context(), key).Result()
	if err != nil{
		return err
	}
	return nil
}


// renew about the lock before the lock release
func (r *redisLock) renew(ctx context.Context, key string, timeout, renewTime int64) {
	reTime := time.Duration(renewTime*int64(time.Second))
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.Tick(reTime):
			_, _ = r.client.Do(r.client.Context(), "SET", key, 1, "NX", "EX", timeout).Result()
		}
	}
}
