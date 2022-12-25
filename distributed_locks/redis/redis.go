package redis

import (
	"context"
	"errors"
	"github.com/Johnathan-Chan/go-tools/distributed_locks"
	"github.com/Johnathan-Chan/go-tools/logger"
	"github.com/go-redis/redis/v8"
	"sync"
	"sync/atomic"
	"time"
)

var (
	GetLockFailedErr = errors.New("redis the lock failed")
	RenewTimeErr     = errors.New("the renewTime must smaller than timeout")
)

// RedisAgent the redis manager that manage the local key
type RedisAgent struct {
	// store the redis key in local, the local value is the *list.Element
	keyMap sync.Map
	// the redis client
	client *redis.Client
	// agent lock that prevent duplicate creation of objects
	lock sync.Mutex
	rwLock sync.RWMutex
	// manager key
	key string
}

func NewRedisAgent(client *redis.Client, gcTime time.Duration) distributed_locks.Agent {
	agent := &RedisAgent{client: client, key: "redis_agent_manager_lock"}
	go agent.gc(gcTime)
	return agent
}

func (r *RedisAgent) gc(gcTime time.Duration){
	ticker := time.Tick(gcTime)
	for range ticker{
		// get the write lock prevent to read lock
		r.rwLock.Lock()
		r.keyMap.Range(func(key, value interface{}) bool {
			if lock, ok := value.(*redisLock); ok {
				if lock.active <= 0 {
					r.keyMap.Delete(key)
					logger.ConsoleWarn("lockGc", key)
				}
			}
			return true
		})
		r.rwLock.Unlock()
	}
}

// Lock obtain the lock
func (r *RedisAgent) Lock(key string, timeout, renewTime int64) error {
	// the renewTime must smaller than timeout
	if timeout <= renewTime {
		return RenewTimeErr
	}

	// Get the read lock prevent to gc
	r.rwLock.RLock()
	defer r.rwLock.RUnlock()

	// Get the local lock
	lockObj, ok := r.keyMap.Load(key)
	if !ok {
		// prevent duplicate creation of objects
		r.lock.Lock()

		lockObj, ok = r.keyMap.Load(key)
		if !ok {
			lockObj = newRedisLock(key, r.client)
			r.keyMap.Store(key, lockObj)
		}

		// prevent to stop other lock
		r.lock.Unlock()
	}

	// distributed lock to local lock
	redisLockObj := lockObj.(*redisLock)

	// get redis lock
	newContext, cancelFunc := context.WithCancel(r.client.Context())
	redisLockObj.obtainLock(newContext, cancelFunc, key, timeout, renewTime)
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
}

// redisLock redis lock map to the lock mutex
type redisLock struct {
	key string
	lock       sync.Mutex
	client     *redis.Client
	ctx        context.Context
	cancelFunc context.CancelFunc
	active     int64
}

func newRedisLock(key string, client *redis.Client) *redisLock {
	return &redisLock{key: key, client: client}
}

// obtainLock get the redis lock
func (r *redisLock) obtainLock(ctx context.Context, cancelFunc context.CancelFunc, key string, timeout, renewTime int64) {

	// if want to get lock, it will be active
	atomic.AddInt64(&r.active, 1)

	// get local lock
	r.lock.Lock()

	// new context
	r.ctx = ctx
	r.cancelFunc = cancelFunc

	var (
		result interface{}
		err error
	)

	// it will keep acquiring locks until you know success
	for result, err = r.client.Do(r.client.Context(), "SET", key, 1, "NX", "EX", timeout).Result();
		result != "OK" || err != nil;{

		result, err = r.client.Do(r.client.Context(), "SET", key, 1, "NX", "EX", timeout).Result()
		if result == "OK" && err == nil {
			break
		}
	}

	if result == "OK" && err == nil {
		// if get the redis lock success it will be go to renew
		go r.renew(r.ctx, key, timeout, renewTime)
	}
}

// releaseLock release the redis lock
func (r *redisLock) releaseLock(key string) error {
	_, err := r.client.Del(r.client.Context(), key).Result()
	if err != nil{
		return err
	}

	atomic.AddInt64(&r.active, -1)

	// release the local lock
	r.lock.Unlock()
	return nil
}


// renew about the lock before the lock release
func (r *redisLock) renew(ctx context.Context, key string, timeout, renewTime int64) {
	reTime := time.Duration(renewTime*int64(time.Second))
	ticker := time.NewTicker(reTime)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = r.client.Expire(r.client.Context(), key, time.Duration(timeout*int64(time.Second))).Result()
		}
	}
}
