package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
)

// Producer ...
type Producer struct {
	ap     sarama.AsyncProducer
	codec  Codec
	closed bool
}

// NewProducer ...
func NewProducer(hosts []string, auth AuthStrategy, options ...Option) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	options = append([]Option{
		// default
		auth.AuthConfig(),
		WithProductRequiredAcks(sarama.WaitForAll),
		WithProductPartitioner(sarama.NewRandomPartitioner),
		WithProductTimeout(time.Second * 1),
		WithKafkaVersion(sarama.V2_4_0_0),
		// options can cover the default
	}, options...)

	// set the custom option
	for _, o := range options {
		o(cfg)
	}

	p, err := sarama.NewAsyncProducer(hosts, cfg)
	if err != nil {
		return nil, err
	}

	producer := &Producer{ap: p, codec: DefaultCodec}
	go producer.run()
	return producer, nil
}

// Run ...
func (p *Producer) run() {
	success := p.ap.Successes()
	errors := p.ap.Errors()
	defer log.Println("producer loop stop")

	for {
		select {
		case _, ok := <-success:
			if !ok {
				return
			}
		case err, ok := <-errors:
			if !ok {
				log.Println("produce message fail, error: ", err.Error())
				return
			}
		}
	}
}

// SetCodec ...
func (p *Producer) SetCodec(codec Codec) {
	p.codec = codec
}

// Publish ...
func (p *Producer) Publish(topic string, data interface{}) error {
	if p.closed {
		return ErrAlreadyClosed
	}

	encodeData, err := p.codec.Marshal(data)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.ByteEncoder(encodeData)

	p.ap.Input() <- msg
	return nil
}

// PublishString ...
func (p *Producer) PublishString(topic, message string) error {
	if p.closed {
		return ErrAlreadyClosed
	}

	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(message)

	p.ap.Input() <- msg
	return nil
}

// PublishRawMsg ...
func (p *Producer) PublishRawMsg(msg *sarama.ProducerMessage) error {
	if p.closed {
		return ErrAlreadyClosed
	}

	p.ap.Input() <- msg
	return nil
}

// Close ...
func (p *Producer) Close() error {
	p.closed = true
	return p.ap.Close()
}