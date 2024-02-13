package pkg

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/cheggaaa/pb/v3"
)

// string of topics contains: topics.name-partition-replicas(brokerId)

type Cluster struct {
	Brokers         []Topics
	NumberOfBrokers int
}
type Topics struct {
	Topic   map[int]string
	Leaders int
}

// Returning list of brokers with topic.name-partitions-replicaAssigment
func (c *Cluster) GetCurrentBalance(admin sarama.ClusterAdmin, from int) (err error) {

	var counter int = 0
	log.Println("Start getting current assigment")
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if len(c.Brokers) == 0 {
		log.Println(len(c.Brokers))
		c.Brokers = make([]Topics, c.NumberOfBrokers)
	}

	for i := 0; i < c.NumberOfBrokers; i++ {
		c.Brokers[i].Topic = make(map[int]string)
	}
	// if --from not seted (equal -1) - geting current assign for all topics
	if from == -1 {
		for k, i := range topics {
			for p, bs := range i.ReplicaAssignment {
				for l, b := range bs {
					c.Brokers[b].Topic[counter] = fmt.Sprintf("%s-%d-%d", k, p, l+1)
					if l+1 == 1 {
						c.Brokers[b].Leaders += 1
					}
					counter++
				}
			}
		}
	} else {
		for k, i := range topics {
			replicaAssigment := i.ReplicaAssignment
			if !searchForMove(replicaAssigment, int32(from)) {
				continue
			}
			for p, bs := range replicaAssigment {
				for l, b := range bs {
					c.Brokers[b].Topic[counter] = fmt.Sprintf("%s-%d-%d", k, p, l+1)
					if l+1 == 1 {
						c.Brokers[b].Leaders += 1
					}
					counter++
				}
			}
		}
	}
	for i, v := range c.Brokers {
		log.Printf("Before rebalance inside broker %d contains %d partitions", i, len(v.Topic))
		log.Printf("For broker %d before rebalance number by leaders %d", i, v.Leaders)
		fmt.Println()
	}
	return nil
}

func (c Cluster) CreateRebalancePlane(to []int) (result Cluster, err error) {
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
	result, err = makePlane(allTopicsSort, c.NumberOfBrokers, to)
	if err != nil {
		return result, nil
	}
	return result, nil
}

func (c Cluster) Rebalance(admin sarama.ClusterAdmin, numberOfTopics int) (err error) {
	var (
		counter float32
	)
	fmt.Println()
	log.Println("Start rebalance...")
	// log.Println(c)
	plane, err := c.ExtructPlane(numberOfTopics)
	// _, err = c.ExtructPlane(numberOfTopics)
	if err != nil {
		return err
	}

	counter = float32(len(plane))
	bar := pb.StartNew(int(counter))
	defer bar.Finish()
	for k, v := range plane {
		err = admin.AlterPartitionReassignments(k, v)
		if err != nil {
			return err
		}
		bar.Increment()
	}
	return nil
}
