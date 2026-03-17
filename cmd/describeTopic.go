package cmd

import (
	"fmt"

	"github.com/IBM/sarama"
)

// Return current assign topics with format '<topic_name>-<partition_number>-<role>'
// where role its a position in assign. 1 - leader, 2 or mode - replicas
func (c *Cluster) DescribeTopic(admin sarama.ClusterAdmin, topic []string) (err error) {
	var counter int
	metadata, err := admin.DescribeTopics(topic)
	if err != nil {
		return err
	}
	if c.Brokers == nil {
		c.Brokers = make(map[int32]Topics)
	}
	for _, topicMetadata := range metadata {
		for _, partitions := range topicMetadata.Partitions {
			for i, brokerId := range partitions.Replicas {
				topicString := fmt.Sprintf("%s-%d-%d", topicMetadata.Name, partitions.ID, i+1)
				t, exist := c.Brokers[brokerId]
				if !exist {
					t = Topics{
						Topic: make(map[int]string),
					}
				}
				if t.Topic == nil {
					t.Topic = make(map[int]string)
				}
				t.Topic[counter] = topicString
				c.Brokers[brokerId] = t
				counter++
			}
		}
	}
	return nil
}
