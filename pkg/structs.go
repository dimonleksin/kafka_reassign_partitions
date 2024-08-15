package pkg

import "github.com/IBM/sarama"

/*
/* string of topics contains: topics.name-partition-replicas
/* where replicas is a broker Id
*/

type Cluster struct {
	Brokers         []Topics
	NumberOfBrokers int
}
type Topics struct {
	Topic   map[int]string
	Leaders int
}

type Settings struct {
	MoveSetting             MoveSettings   `yaml:"move-params"`
	BootstrapSettings       BrokerSettings `yaml:"kafka"`
	KafkaApiVersionFormated sarama.KafkaVersion
	H                       *bool
	Help                    *bool
	Version                 *bool
}

type BrokerSettings struct {
	BrokersS        *string
	KafkaApiVersion *string          `yaml:"api-version"`
	Security        SecuritySettings `yaml:"security"`
	Brokers         []string         `yaml:"bootstrap-server"`
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
	From   *int  `yaml:"from"`
	To     []int `yaml:"to"`
	ToS    *string
	TopicS *string
	Treads *int
	Action *string  `yaml:"action"`
	Topics []string `yaml:"topics"`
}
