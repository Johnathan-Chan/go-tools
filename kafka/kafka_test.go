package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"testing"
)

func TestConsumer(t *testing.T) {
	kafka := NewKafka(
		WithHosts([]string{"douying-kafka:9092"}),
		WithAuth(new(NoAuth)),
	)

	consumer, err := kafka.CreateConsumerGroup([]string{"douyin"}, "test_consumer", new(NoAutoCommitOffset))
	if err != nil{
		log.Println(err)
		return
	}

	consumer.Run(func(message *sarama.ConsumerMessage, commit CommitHandler) {
		log.Printf("partition:%d, offset:%d, value:%s", message.Partition, message.Offset, message.Value)
		commit()
	})

	for{

	}
}

func TestProducer(t *testing.T) {
	kafka := NewKafka(
		WithHosts([]string{"local-kafka:9092"}),
		WithAuth(&GSSAPIAuth{
			Username: "kafka/local-kafka",
			ServiceName: "kafka",
			Realm: "KAFKA.COM",
			KeyTabPath: "D:/store/kafka.keytab",
			KerberosConfigPath: "D:/store/krb5.conf",
		}),
	)

	producer, err := kafka.CreateProducer()
	if err != nil{
		log.Println(err)
		return
	}

	for index:=0; index < 10; index++{
		//time.Sleep(time.Second)
		err := producer.PublishString("douyin", `{"data":null,"database":"test","es":0,"mysqlType":null,"old":null,"table":"test","ts":0,"type":""}`)
		if err != nil{
			log.Println(err)
		}
		log.Println("producer:", index)
	}

	for {

	}
}

func TestGSSAPIAuthConsumer(t *testing.T) {
	kafka := NewKafka(
		WithHosts([]string{"local-kafka:9092"}),
		WithAuth(&GSSAPIAuth{
			Username: "kafka/local-kafka",
			ServiceName: "kafka",
			Realm: "KAFKA.COM",
			KeyTabPath: "D:/store/kafka.keytab",
			KerberosConfigPath: "D:/store/krb5.conf",
		}),
	)

	consumer, err := kafka.CreateConsumerGroup([]string{"douyin"}, "test_consumer", new(NoAutoCommitOffset))
	if err != nil{
		log.Println(err)
		return
	}

	consumer.Run(func(message *sarama.ConsumerMessage, commit CommitHandler) {
		log.Printf("topic: %s, partition:%d, offset:%d, value:%s", message.Topic, message.Partition, message.Offset, message.Value)
		commit()
	})

	for{

	}
}