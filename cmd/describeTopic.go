package cmd

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
		c.Brokers = make(map[int]Topics)
	}
	// ensure Topic maps exist for known brokers
	for id := range c.Brokers {
		t := c.Brokers[id]
		if t.Topic == nil {
			t.Topic = make(map[int]string)
			c.Brokers[id] = t
		}
	}
	for _, topicMetadata := range metadata {
		for _, partitions := range topicMetadata.Partitions {
			for i, p := range partitions.Replicas {
				topicString := fmt.Sprintf("%s-%d-%d", topicMetadata.Name, partitions.ID, i+1)
				bID := int(p)
				t := c.Brokers[bID]
				if t.Topic == nil {
					t.Topic = make(map[int]string)
				}
				t.Topic[counter] = topicString
				c.Brokers[bID] = t
				counter++
			}
		}
	}
	return nil
}
