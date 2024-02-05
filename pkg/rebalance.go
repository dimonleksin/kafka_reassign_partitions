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
	// delete(c.Brokers, 0)
	// c.Brokers = make(map[int32]Topics)
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
		allTopics    []string
		leaders      int
		l            int
		avg          int
		tmp          int
		co           int
		co2          int
		avgByLeaders int
		tmpResult    Cluster
	)
	for i := 0; i < 5; i++ {
		result.Brokers[i].Topic = make(map[int]string)
	}
	for i := 0; i < 5; i++ {
		tmpResult.Brokers[i].Topic = make(map[int]string)
	}
	// var counter int = 0
	for _, v := range c.Brokers {
		for _, t := range v.Topic {
			allTopics = append(allTopics, t)
		}
		leaders += v.Leaders
	}
	l = len(allTopics) //Number of topics
	avg = l / 4
	avgByLeaders = avg / 3
	log.Println(avgByLeaders)
	co = 1
	co2 = 1
	tmp = avg
	for i := 0; i < l; i++ {
		if i >= tmp && tmp < avg*4 {
			tmp += avg
			co++
		}

		if co2 >= 4 {
			co2 = 1
		}

		currentRole, err := strconv.Atoi(strings.Split(allTopics[i], "-")[len(strings.Split(allTopics[i], "-"))-1])
		if err != nil {
			return result, err
		}
		if tmpResult.Brokers[co].Leaders >= avgByLeaders && currentRole == 1 {
			if co < 4 {
				tmpResult.Brokers[co+1].Topic[i] = allTopics[i]
				tmpResult.Brokers[co+1].Leaders += 1
			} else {
				tmpResult.Brokers[co2].Topic[i] = allTopics[i]
				tmpResult.Brokers[co2].Leaders += 1
				co2++
			}
			// if currentRole == 1 {

			// }

		} else {
			tmpResult.Brokers[co].Topic[i] = allTopics[i]

			// if err != nil {
			// 	return result, err
			// }
			if currentRole == 1 {
				tmpResult.Brokers[co].Leaders += 1
			}
		}
	}
	result = tmpResult
	return result, nil
}

// func (c Cluster) Rebalance(palne Cluster, admin sarama.ClusterAdmin) error {

// err := admin.AlterPartitionReassignments()
// if err != nil {
// 	return err
// }
// 	return nil
// }
