package pkg

import (
	"fmt"

	"github.com/IBM/sarama"
)

// Return current assign topics with format '<topic_name>-<partition_number>-<role>'
func (c *Cluster) DescribeTopic(admin sarama.ClusterAdmin, topic []string) (err error) {
	var counter int
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
	for _, topicMetadata := range metadata {
		for _, partitions := range topicMetadata.Partitions {
			for i, p := range partitions.Replicas {
				topicString := fmt.Sprintf("%s-%d-%d", topicMetadata.Name, partitions.ID, i+1)
				c.Brokers[p].Topic[counter] = topicString
				counter++
			}
		}
	}
	return nil
}
