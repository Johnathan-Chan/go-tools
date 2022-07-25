package redis

import (
	"github.com/go-redis/redis/v8"
	"log"
	"strconv"
	"testing"
	"time"
)

func TestRedisAgent(t *testing.T) {
	redisOptions := &redis.Options{
		Addr:         "172.16.11.200:6379",
		Password:     "",
		DB:           0,
		PoolSize:     5,
		MinIdleConns: 100,
		IdleTimeout:  60000000000,
	}

	rdb := redis.NewClient(redisOptions)
	ragent := NewRedisAgent(rdb, time.Second*5)

	key := "test"
	i := 1000
	for a, b:=0, 0; ;a++ {
		time.Sleep(time.Nanosecond)
		go func(innerKey string) {
			targetKey := innerKey + strconv.Itoa(a % 4)
			if err := ragent.Lock(targetKey, 10, 5); err == nil{
				defer ragent.UnLock(targetKey)
			}

			i = a+b
			time.Sleep(time.Millisecond * 500)
			log.Println(i)
		}(key)
	}
}