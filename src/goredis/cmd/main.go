package main

import (
	"fmt"
	"log"

	"goredis/redis"
)

type RedisConfig struct {
	Addr     string
	PoolSize int32
}

func main() {
	rc := &RedisConfig{"localhost:6379", 1000}
	client, err := initRedisClient(rc)
	if err != nil {
		log.Fatal(err)
	}

	key := "aa"
	err = client.Set(key, []byte("bb"))
	if err != nil {
		log.Fatal(err)
	}

	value, err := client.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("value: ", string(value))
}

func initRedisClient(rc *RedisConfig) (client *redis.RedisClient, err error) {
	if rc == nil {
		err = fmt.Errorf("In InitRedisClient() RedisConfig is nil")
		return
	}
	client, err = redis.New(rc.Addr, 0, int(rc.PoolSize))
	return
}
