package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	kafka := NewKafka(
		WithHosts([]string{"douying-kafka:9092"}),
		WithAuth(new(NoAuth)),
	)

	consumer, err := kafka.CreateConsumerGroup([]string{"test"}, "test_consumer", new(NoAutoCommitOffset))
	if err != nil{
		log.Println(err)
		return
	}

	consumer.Run(func(message *sarama.ConsumerMessage) {
		log.Printf("partition:%d, offset:%d, value:%s", message.Partition, message.Offset, message.Value)
	})

	for{

	}
}

func TestProducer(t *testing.T) {
	kafka := NewKafka(
		WithHosts([]string{"douying-kafka:9092"}),
		WithAuth(new(NoAuth)),
	)

	producer, err := kafka.CreateProducer()
	if err != nil{
		log.Println(err)
		return
	}

	for index:=0; index < 1000; index++{
		time.Sleep(time.Second)
		producer.Publish("test", index)
		log.Println("producer:", index)
	}
}
