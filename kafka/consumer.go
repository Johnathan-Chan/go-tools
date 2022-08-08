package kafka

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Consumer ...
type Consumer struct {
	cg        sarama.ConsumerGroup
	client    sarama.Client
	cancel    context.CancelFunc
	group     string
	Topics    []string
	Reconnect time.Duration
	CommitOffsetStrategy
}

// NewConsumer ...
func NewConsumer(hosts, topics []string, groupName string, auth AuthStrategy, strategy CommitOffsetStrategy,
	options ...Option) (*Consumer, error) {

	cfg := sarama.NewConfig()
	options = append(options, strategy.Config())
	options = append([]Option{
		auth.AuthConfig(),
		WithConsumerOffsets(sarama.OffsetOldest),
		WithConsumerSessionTimeout(10 * time.Second),
		WithConsumerHeartbeatInterval(5 * time.Second),
		WithConsumerGroupRebalanceStrategy(sarama.BalanceStrategyRange, 30 * time.Second, 2 * time.Second, 5),
		WithKafkaVersion(sarama.V2_4_0_0),
	}, options...)
	for _, o := range options {
		o(cfg)
	}

	client, err := sarama.NewClient(hosts, cfg)
	if err != nil {
		return nil, err
	}

	cg, err := sarama.NewConsumerGroupFromClient(groupName, client)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Topics:    topics,
		cg:        cg,
		client:    client,
		group:     groupName,
		Reconnect: cfg.Consumer.Group.Heartbeat.Interval,
		CommitOffsetStrategy: strategy,
	}, nil
}

// Run ...
func (c *Consumer) Run(handler Handler) {
	c.run(NewConsumHandler(handler, c.CommitOffsetStrategy))
}

func (c *Consumer) run(handler sarama.ConsumerGroupHandler) {
	if c.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	go func() {
		log.Println("kafka consume start...")
		for {
			if err := c.cg.Consume(
				context.Background(),
				c.Topics,
				handler,
			); err != nil && !errors.Is(err, io.EOF) {
				log.Println("kafka consume fail, error: ", err.Error())
				time.Sleep(c.Reconnect)
			}

			select {
			case <-ctx.Done():
				log.Println("kafka consume stop...")
				return
			default:
				log.Println("kafka consume retry...")
			}

		}
	}()
}

func (c *Consumer) DeleteGroup() error {
	admin, err := sarama.NewClusterAdminFromClient(c.client)
	if err != nil {
		return err
	}

	return admin.DeleteConsumerGroup(c.group)
}

// Close ...
func (c *Consumer) Close() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.cg.Close()
}

type CommitHandler func()
type Handler func(*sarama.ConsumerMessage, CommitHandler)

// ConsumHandler ...
type ConsumHandler struct {
	handler Handler
	strategy CommitOffsetStrategy
}

func NewConsumHandler(handler Handler, strategy CommitOffsetStrategy) *ConsumHandler{
	return &ConsumHandler{handler: handler, strategy: strategy}
}

// ConsumeClaim ..
func (h *ConsumHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.handler(msg, func() {
			// if you set the no auto commit, you have to commit by yourself
			h.strategy.Commit(sess)
		})
		sess.MarkMessage(msg, "")
	}
	return nil
}

// Setup ..
func (h *ConsumHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup ..
func (h *ConsumHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}