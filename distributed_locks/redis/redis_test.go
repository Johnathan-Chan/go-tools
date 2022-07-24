package redis

import (
	"github.com/go-redis/redis/v8"
	"log"
	"testing"
	"time"
)

func TestRedisAgent(t *testing.T) {
	redisOptions := &redis.Options{
		Addr:         "192.168.132.128:6379",
		Password:     "",
		DB:           0,
		PoolSize:     5,
		MinIdleConns: 100,
		IdleTimeout:  60000000000,
	}

	rdb := redis.NewClient(redisOptions)
	ragent := NewRedisAgent(rdb)

	key := "test"
	i := 1000
	for a, b:=0, 0; ;a++ {
		time.Sleep(time.Microsecond * 1)
		go func() {
			if err := ragent.Lock(key, 10, 5); err == nil{
				defer ragent.UnLock(key)
			}

			i = a+b
			log.Println(i)
		}()
	}
}