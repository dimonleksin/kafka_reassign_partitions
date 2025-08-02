package settings

import (
	"github.com/IBM/sarama"
	"github.com/dimonleksin/kafka_reasign_partition/internal/stuff/event"
)

type Settings struct {
	MoveSetting             MoveSettings   `yaml:"move-params"`
	BootstrapSettings       BrokerSettings `yaml:"kafka"`
	KafkaApiVersionFormated sarama.KafkaVersion
	H                       *bool // equal help
	Help                    *bool
	Version                 *bool
	Verbose                 bool // verbose output
}

type BrokerSettings struct {
	BrokersS        *string          `yaml:"bootstrap-server"`
	KafkaApiVersion *string          `yaml:"api-version"`
	Security        SecuritySettings `yaml:"security"`
	Brokers         []string         `yaml:"-"`
}

type SecuritySettings struct {
	User      *string `yaml:"user"`
	Passwd    *string `yaml:"password"`
	Mechanism *string `yaml:"mechanism"`
	Protocol  *string `yaml:"protocol"`
	Tls       TLS     `yaml:"tls"`
}

type TLS struct {
	UseTLS   *bool   `yaml:"enable"`
	CAPath   *string `yaml:"ca"`
	CertPath *string `yaml:"cert"`
	KeyPath  *string `yaml:"key"`
}

type MoveSettings struct {
	From          *int          `yaml:"from"`
	To            []int         `yaml:"-"`
	ToS           *string       `yaml:"to"`
	TopicS        *string       `yaml:"-"`
	Action        event.Actions `yaml:"action"`
	Topics        []string      `yaml:"topics"`
	Sync          bool          `yaml:"sync"`           // if true - await finaly rebalase before work with next topic
	BackupVersion int           `yaml:"backup-version"` // version of backup for restore
	Treads        *int
}
