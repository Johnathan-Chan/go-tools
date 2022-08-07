package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"time"
)

var (
	ErrAlreadyClosed = errors.New("the producer already closed")
)

type Option func(config *sarama.Config)

// global config

func WithKafkaVersion(version sarama.KafkaVersion) Option {
	return func(config *sarama.Config) {
		config.Version = version
	}
}

// producer config

func WithProductRequiredAcks(ack sarama.RequiredAcks) Option{
	return func(config *sarama.Config) {
		config.Producer.RequiredAcks = ack
	}
}

func WithProductPartitioner(constructor sarama.PartitionerConstructor) Option{
	return func(config *sarama.Config) {
		config.Producer.Partitioner = constructor
	}
}

func WithProductTimeout(timeout time.Duration) Option{
	return func(config *sarama.Config) {
		config.Producer.Timeout = timeout
	}
}

// consumer config

func WithConsumerOffsets(offset int64) Option{
	return func(config *sarama.Config) {
		config.Consumer.Offsets.Initial = offset
	}
}

func WithConsumerSessionTimeout(timeout time.Duration) Option{
	return func(config *sarama.Config) {
		config.Consumer.Group.Session.Timeout = timeout
	}
}

func WithConsumerHeartbeatInterval(interval time.Duration) Option{
	return func(config *sarama.Config) {
		config.Consumer.Group.Heartbeat.Interval = interval
	}
}

func WithConsumerGroupRebalanceStrategy(strategy sarama.BalanceStrategy, timeout, backoff time.Duration,
	retryMax int) Option {
	return func(config *sarama.Config) {
		config.Consumer.Group.Rebalance.Strategy = strategy
		config.Consumer.Group.Rebalance.Timeout = timeout
		config.Consumer.Group.Rebalance.Retry.Max = retryMax
		config.Consumer.Group.Rebalance.Retry.Backoff = backoff
	}
}