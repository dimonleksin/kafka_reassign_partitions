package pkg

// string of topics contains: topics.name-partition-replicas(brokerId)

type Cluster struct {
	Brokers         []Topics
	NumberOfBrokers int
}
type Topics struct {
	Topic   map[int]string
	Leaders int
}

type Settings struct {
	BrokersS *string
	Brokers  []string
	Action   *string
	User     *string
	Passwd   *string
	From     *int
	ToS      *string
	To       []int
	H        *bool
	Help     *bool
	TopicS   *string
	Topics   []string
	Treads   *int
}
