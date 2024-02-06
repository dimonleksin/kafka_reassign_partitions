package pkg

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

// string of topics contains: topics.name-partition-replicas(brokerId)

type Cluster struct {
	Brokers [5]Topics
}
type Topics struct {
	Topic   map[int]string
	Leaders int
}

// Returning list of brokers with topic.name-partitions-replicaAssigment
func (c *Cluster) GetCurrentBalance(admin sarama.ClusterAdmin) (err error) {

	var counter int = 0
	log.Println("Start getting current assigment")
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}
	for i := 0; i < 5; i++ {
		c.Brokers[i].Topic = make(map[int]string)
	}

	for k, i := range topics {
		for p, bs := range i.ReplicaAssignment {
			// c := len(bs)
			for l, b := range bs {

				// c.Topics[counter] = fmt.Sprintf("%s-%d-%d", k, p, b)
				// c.Brokers[b] = Topics{Topic: make(map[int]string)}
				c.Brokers[b].Topic[counter] = fmt.Sprintf("%s-%d-%d", k, p, l+1)
				if l+1 == 1 {
					c.Brokers[b].Leaders += 1
				}
				counter++
			}
		}
	}
	for i, v := range c.Brokers {
		// for _, t := range v.Topic {
		// 	log.Printf("Before rebalance inside broker %d contains %s partitions", i, t)
		// }
		log.Printf("Before rebalance inside broker %d contains %d partitions", i, len(v.Topic))
		log.Printf("For broker %d before rebalance number by leaders %d", i, v.Leaders)
		fmt.Println()
	}
	return nil
}

func (c Cluster) CreateRebalancePlane() (result Cluster, err error) {
	var (
		allTopics     map[int]string
		allTopicsSort map[int]string
		leaders       int
		counter       int
	)

	allTopics = make(map[int]string)

	for _, v := range c.Brokers {
		for _, t := range v.Topic {
			allTopics[counter] = t
			counter++
		}
		leaders += v.Leaders
	}

	allTopicsSort, err = sortTopicMap(allTopics)
	if err != nil {
		return result, err
	}

	// log.Printf("Sorted topic list: %v", allTopicsSort)

	result, err = makePlane(allTopicsSort)
	if err != nil {
		return result, nil
	}

	return result, nil
}

func (c Cluster) Rebalance(admin sarama.ClusterAdmin) error {
	var (
		topic       string
		partitionID int
		err         error
		// assigment   []int32
	)

	assigments := make(map[string][][]int32)

	for i := 1; i < len(c.Brokers); i++ {
		for _, t := range c.Brokers[i].Topic {
			tmp := strings.Split(t, "-")
			topic = tmp[0]
			partitionID, err = strconv.Atoi(tmp[1])
			if err != nil {
				return err
			}
			if len(assigments[topic]) == 0 {
				// log.Println("Make slices")
				assigments[topic] = make([][]int32, 63)
			}
			if len(assigments[topic][partitionID]) == 0 {
				assigments[topic][partitionID] = make([]int32, 5)
			}
			assigments[topic][partitionID][i] = int32(i)

		}
		// log.Printf("Iteration %d", i)
		// log.Println(assigments[topic])

	}
	for k, v := range assigments {
		log.Printf("For topic: %s\nassigment:%v", k, v)
	}

	// err := admin.AlterPartitionReassignments()
	if err != nil {
		return err
	}
	return nil
}
