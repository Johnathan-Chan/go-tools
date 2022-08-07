package kafka

type option func(*Kafka)

func WithHosts(Host []string) option{
	return func(kafka *Kafka) {
		kafka.Host = Host
	}
}

func WithAuth(auth AuthStrategy) option{
	return func(kafka *Kafka) {
		kafka.AuthStrategy = auth
	}
}

type Kafka struct {
	Host []string
	AuthStrategy
}

func NewKafka(options ...option) *Kafka {
	config := &Kafka{}
	for _, op := range options{
		op(config)
	}
	return config
}

func (k *Kafka) CreateProducer(options ...Option) (*Producer, error){
	return NewProducer(k.Host, k.AuthStrategy, options...)
}

func (k *Kafka) CreateConsumerGroup(topics []string, groupName string, strategy CommitOffsetStrategy,
	options ...Option) (*Consumer, error) {
	return NewConsumer(k.Host, topics, groupName, k.AuthStrategy, strategy, options...)
}