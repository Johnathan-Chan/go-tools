package kafka

import (
	"github.com/Shopify/sarama"
	"time"
)

type AuthStrategy interface {
	AuthConfig() Option
}

type NoAuth uint8

func (n *NoAuth) AuthConfig() Option {
	return func(config *sarama.Config) {}
}

type PlainAuth struct {
	Username string
	Password string
}

func (p *PlainAuth) AuthConfig() Option {
	return func(config *sarama.Config) {
		config.Net.SASL.Mechanism = "PLAIN"
		config.Net.SASL.Enable = true
		config.Net.SASL.User = p.Username
		config.Net.SASL.Password = p.Password
	}
}

type GSSAPIAuth struct {
	Username string
	ServiceName string
	Realm string
	KeyTabPath string
	KerberosConfigPath string
}

func (g *GSSAPIAuth) AuthConfig() Option {
	return func(config *sarama.Config) {
		config.Net.SASL.Enable = true
		config.Metadata.AllowAutoTopicCreation = true
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
		config.Net.SASL.GSSAPI.Username = g.Username
		config.Net.SASL.GSSAPI.ServiceName = g.ServiceName
		config.Net.SASL.GSSAPI.Realm = g.Realm
		config.Net.SASL.GSSAPI.KeyTabPath = g.KeyTabPath
		config.Net.SASL.GSSAPI.KerberosConfigPath = g.KerberosConfigPath
	}
}

type CommitOffsetStrategy interface {
	Config() Option
	Commit(sarama.ConsumerGroupSession)
}

type AutoCommitOffset struct {
	Interval time.Duration
}

func (a *AutoCommitOffset) Config() Option {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.AutoCommit.Enable = true
		config.Consumer.Offsets.AutoCommit.Interval = a.Interval
	}
}

func (a *AutoCommitOffset) Commit(sarama.ConsumerGroupSession) {}

type NoAutoCommitOffset int8

func (n NoAutoCommitOffset) Config() Option {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}
}

func (n NoAutoCommitOffset) Commit(sess sarama.ConsumerGroupSession) {
	sess.Commit()
}



