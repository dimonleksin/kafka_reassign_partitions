package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

// Sorting map of topics by index of leaders/replicas
// Leaders in fearst numbers
func sortTopicMap(topics map[int]string) (sortedTopics map[int]string, err error) {
	var (
		tmp       map[int]string
		counter_1 int
		counter_2 int
		l         int
	)

	sortedTopics = make(map[int]string)
	tmp = make(map[int]string)

	for _, v := range topics {
		currentRole, err := strconv.Atoi(strings.Split(v, "-")[len(strings.Split(v, "-"))-1])
		if err != nil {
			return nil, err
		}
		if currentRole == 1 {
			sortedTopics[counter_1] = v
			counter_1++
			continue
		}
		tmp[counter_2] = v
		counter_2++
	}
	l = len(sortedTopics)

	// Extended sortedTopics from tmp
	len_tmp := len(tmp)
	for i := 0; i < len_tmp; i++ {
		ind := l + i
		sortedTopics[ind] = tmp[i]
	}

	return sortedTopics, nil
}

// Shufle current broker in broker list from --to for uniform reasign
func shufleCounter(to []int32) []int32 {
	tmp := to[0]
	for i := 0; i < len(to)-1; i++ {
		to[i] = to[i+1]
	}
	to[len(to)-1] = tmp
	return to
}

func initBrokerList(brokers map[int32]Topics, counter int32) *Topics {
	t, exist := brokers[counter]
	if !exist {
		t = Topics{
			Topic: make(map[int]string),
		}
	}
	if t.Topic == nil {
		t.Topic = make(map[int]string)
	}
	return &t
}

// Maked plane for rebalance | nob - Number Of Brokers
func makePlane(topics map[int]string, nob int32, to []int32) (result Cluster, err error) {
	var counter int32 = 1
	if to != nil {
		counter = to[0]
	}
	if result.Brokers == nil {
		result.Brokers = make(map[int32]Topics)
	}
	// not range, because neded received topic in ascending order or sorting not working
	for i, topic := range topics {
		if counter > nob {
			counter = 1
		}
		if to != nil {
			counter = to[0]
		}
		// Getting role from topic: 1 - leader, 2 and other - replicas
		currentRole, err := strconv.Atoi(strings.Split(topic, "-")[len(strings.Split(topic, "-"))-1])
		if err != nil {
			return result, err
		}
		t := *initBrokerList(result.Brokers, counter)
		// increment counter until broker with current replicas not fount for avoid dublications
		for search(t.Topic, topic[0:len(topic)-2]) {
			// if --to not set, reasign for all brokers
			if to == nil {
				counter++
			} else {
				// if --to seted, reasign to brokers from --to
				counter = to[0]
				to = shufleCounter(to)
			}
			if counter > nob {
				counter = 1
			}
			t = *initBrokerList(result.Brokers, counter)
		}
		if currentRole == 1 {
			t.Leaders += 1
		}
		t.Topic[i] = topic
		result.Brokers[counter] = t
		if to == nil {
			counter++
		} else {
			counter = to[0]
			to = shufleCounter(to)
		}
	}
	fmt.Println(result)
	return result, nil
}

func (c Cluster) ExtructPlane(numberOfTopics int) (plane map[string][][]int32, err error) {
	var (
		topic       string
		partitionID int
		positionID  int
	)
	fmt.Println("Starting executing plane")
	assigments := make(map[string][][]int32)

	for brokerId, _ := range c.Brokers {
		for _, t := range c.Brokers[brokerId].Topic {
			topic, partitionID, positionID, err = parsTopicParams(t)

			if err != nil {
				return nil, err
			}

			if len(assigments[topic]) == 0 {
				assigments[topic] = make([][]int32, numberOfTopics)
			}
			if len(assigments[topic][partitionID]) == 0 {
				assigments[topic][partitionID] = make([]int32, 5)
			}
			assigments[topic][partitionID][positionID] = int32(brokerId)
		}
	}
	plane, err = clearZeroValue(assigments)
	if err != nil {
		return nil, err
	}
	return plane, nil
}

// addded number of brokers from cluster to struct
func (c *Cluster) GetNumberOfBrokers(admin sarama.ClusterAdmin) (err error) {
	var (
		brokers []*sarama.Broker
	)
	brokers, _, err = admin.DescribeCluster()
	if err != nil {
		return fmt.Errorf("something happened when i getting metadata with brokers. Err: %v", err)
	}
	c.NumberOfBrokers = int32(len(brokers))
	return nil
}
