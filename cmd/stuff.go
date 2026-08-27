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
func shufleCounter(to []int) []int {
	tmp := to[0]
	for i := 0; i < len(to)-1; i++ {
		to[i] = to[i+1]
	}
	to[len(to)-1] = tmp
	return to
}

// Maked plane for rebalance | brokerIDs - list of available broker ids
func makePlane(topics map[int]string, brokerIDs []int, to []int) (result Cluster, err error) {
	// counterIndex is index into brokerIDs
	counterIndex := 0
	// if --to set, we rotate over 'to' list of broker ids
	useTo := false
	currentToIndex := 0
	if to != nil && len(to) > 0 {
		useTo = true
	}

	// init result.Brokers map and entries
	if len(result.Brokers) == 0 {
		result.Brokers = make(map[int]Topics)
	}
	for _, id := range brokerIDs {
		t := result.Brokers[id]
		if t.Topic == nil {
			t.Topic = make(map[int]string)
		}
		result.Brokers[id] = t
	}

	// iterate topics in index order
	for i := 0; i < len(topics); i++ {
		// determine current broker id
		var currentBrokerID int
		if useTo {
			currentBrokerID = to[currentToIndex]
		} else {
			currentBrokerID = brokerIDs[counterIndex]
		}

		// Getting role from topic: 1 - leader, 2 and other - replicas
		topicParams := strings.Split(topics[i], "-")
		if len(topicParams) < 3 {
			return result, fmt.Errorf("invalid topic format: %s", topics[i])
		}
		currentRole, err := strconv.Atoi(topicParams[len(topicParams)-1])
		if err != nil {
			return result, err
		}

		// increment counter until broker with current replicas not found for avoid duplications
		for search(result.Brokers[currentBrokerID].Topic, topics[i][0:len(topics[i])-2]) {
			if useTo {
				currentToIndex = (currentToIndex + 1) % len(to)
				currentBrokerID = to[currentToIndex]
			} else {
				counterIndex = (counterIndex + 1) % len(brokerIDs)
				currentBrokerID = brokerIDs[counterIndex]
			}
		}

		if currentRole == 1 {
			tmp := result.Brokers[currentBrokerID]
			tmp.Leaders += 1
			result.Brokers[currentBrokerID] = tmp
		}
		// assign topic
		tmp := result.Brokers[currentBrokerID]
		tmp.Topic[i] = topics[i]
		result.Brokers[currentBrokerID] = tmp

		// advance counters
		if useTo {
			currentToIndex = (currentToIndex + 1) % len(to)
		} else {
			counterIndex = (counterIndex + 1) % len(brokerIDs)
		}
	}

	return result, nil
}

func buildReplicaSequence(cluster Cluster) (map[string]map[int][]int32, error) {
	result := make(map[string]map[int][]int32)
	for brokerID, broker := range cluster.Brokers {
		for _, topicEntry := range broker.Topic {
			topicName, partitionID, positionID, err := parsTopicParams(topicEntry)
			if err != nil {
				return nil, err
			}
			if result[topicName] == nil {
				result[topicName] = make(map[int][]int32)
			}
			if result[topicName][partitionID] == nil {
				result[topicName][partitionID] = make([]int32, 0, 5)
			}
			partitionSequence := result[topicName][partitionID]
			if len(partitionSequence) < positionID {
				partitionSequence = append(partitionSequence, make([]int32, positionID-len(partitionSequence))...)
			}
			partitionSequence[positionID-1] = int32(brokerID)
			result[topicName][partitionID] = partitionSequence
		}
	}
	return result, nil
}

func replicaSequenceEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func filterUnchangedPartitions(current, desired Cluster) Cluster {
	currentAssignments, err := buildReplicaSequence(current)
	if err != nil {
		return desired
	}
	desiredAssignments, err := buildReplicaSequence(desired)
	if err != nil {
		return desired
	}

	for topicName, partitions := range desiredAssignments {
		for partitionID, desiredSequence := range partitions {
			currentSequence, ok := currentAssignments[topicName][partitionID]
			if !ok || !replicaSequenceEqual(currentSequence, desiredSequence) {
				continue
			}
			for brokerID, broker := range desired.Brokers {
				for entryID, topicEntry := range broker.Topic {
					topic, partition, _, err := parsTopicParams(topicEntry)
					if err == nil && topic == topicName && partition == partitionID {
						delete(desired.Brokers[brokerID].Topic, entryID)
					}
				}
			}
		}
	}

	for brokerID, broker := range desired.Brokers {
		leaders := 0
		for _, topicEntry := range broker.Topic {
			topicName, partitionID, positionID, err := parsTopicParams(topicEntry)
			if err != nil {
				continue
			}
			if positionID == 1 && topicName != "" && partitionID >= 0 {
				leaders++
			}
		}
		broker.Leaders = leaders
		desired.Brokers[brokerID] = broker
	}

	return desired
}

func (c Cluster) ExtructPlane(numberOfTopics int) (plane map[string][][]int32, err error) {
	var (
		topic       string
		partitionID int
		positionID  int
	)
	fmt.Println("Starting executing plane")
	assigments := make(map[string][][]int32)

	for brokerID, b := range c.Brokers {
		for _, t := range b.Topic {
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
			assigments[topic][partitionID][positionID] = int32(brokerID)
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
	// Build map of brokers keyed by real broker ID
	if len(c.Brokers) == 0 {
		c.Brokers = make(map[int]Topics)
	}
	for _, broker := range brokers {
		id := int(broker.ID())
		t := c.Brokers[id]
		if t.Topic == nil {
			t.Topic = make(map[int]string)
		}
		c.Brokers[id] = t
	}
	c.NumberOfBrokers = len(brokers)
	fmt.Printf("Number of brokers:  %d\n", c.NumberOfBrokers)
	return nil
}
