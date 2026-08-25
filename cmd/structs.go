package cmd

/*
/* string of topics contains: topics.name-partition-replicas
/* where replicas is a broker Id
*/

type Cluster struct {
	Brokers         map[int]Topics
	NumberOfBrokers int
}
type Topics struct {
	Topic   map[int]string
	Leaders int
}
