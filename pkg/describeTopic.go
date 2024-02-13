package pkg

import (
	"fmt"

	"github.com/IBM/sarama"
)

// Return current assign topics with format 'topic.name-partition_number-role'
func (c *Cluster) DescribeTopic(admin sarama.ClusterAdmin, topic []string) (err error) {
	metadata, err := admin.DescribeTopics(topic)
	if err != nil {
		return err
	}
	if len(c.Brokers) == 0 {
		c.Brokers = make([]Topics, c.NumberOfBrokers)
	}
	for i := 0; i < c.NumberOfBrokers; i++ {
		c.Brokers[i].Topic = make(map[int]string)
	}
	for _, v := range metadata {
		for k, partitions := range v.Partitions {
			for i, p := range partitions.Replicas {
				c.Brokers[p].Topic[k] = fmt.Sprintf("%s-%d-%d", v.Name, partitions.ID, i)
			}
		}
	}
	return nil
}
