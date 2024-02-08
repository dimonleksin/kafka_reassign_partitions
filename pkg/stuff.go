package pkg

import (
	"log"
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

// Maked plane for rebalance
func makePlane(topics map[int]string, nob int) (result Cluster, err error) {
	counter := 1

	if len(result.Brokers) == 0 {
		result.Brokers = make([]Topics, nob)
	}

	for i := 0; i < 5; i++ {
		result.Brokers[i].Topic = make(map[int]string)
	}

	for i := 0; i < len(topics); i++ {
		if counter == 5 {
			counter = 1
		}
		currentRole, err := strconv.Atoi(strings.Split(topics[i], "-")[len(strings.Split(topics[i], "-"))-1])
		if err != nil {
			return result, err
		}
		for search(result.Brokers[counter].Topic, topics[i][0:len(topics[i])-2]) {
			// log.Printf("Founded dublicate: %s", topics[i][0:len(topics[i])-2])
			counter++
			if counter == 5 {
				counter = 1
			}
		}
		if currentRole == 1 {
			result.Brokers[counter].Leaders += 1
		}

		result.Brokers[counter].Topic[i] = topics[i]
		counter++
	}

	return result, nil
}

func (c Cluster) ExtructPlane(numberOfTopics int) (plane map[string][][]int32, err error) {
	var (
		topic       string
		partitionID int
		positionID  int
		l           int
		// assigment   []int32
	)
	log.Println("Starting executing plane")
	assigments := make(map[string][][]int32)

	for i := 1; i < len(c.Brokers); i++ {
		for _, t := range c.Brokers[i].Topic {
			tmp := strings.Split(t, "-")
			l = len(tmp)
			// Geting topic name
			topic = strings.Join(tmp[0:l-2], "-")
			// Geting partition id
			partitionID, err = strconv.Atoi(tmp[l-2])
			// log.Println(topic, l, partitionID)
			if err != nil {
				return nil, err
			}
			// Geting position of reasign
			positionID, err = strconv.Atoi(tmp[l-1])

			if err != nil {
				return nil, err
			}

			if len(assigments[topic]) == 0 {
				// log.Println("Make slices")
				assigments[topic] = make([][]int32, numberOfTopics)
			}
			if len(assigments[topic][partitionID]) == 0 {
				assigments[topic][partitionID] = make([]int32, 5)
			}
			assigments[topic][partitionID][positionID] = int32(i)
		}
	}
	// Deleting zero value
	plane = clearZeroValue(assigments)
	return plane, nil
}

func (c *Cluster) GetNumberOfBrokers(admin sarama.ClusterAdmin) (err error) {
	var (
		brokers []*sarama.Broker
	)
	brokers, _, err = admin.DescribeCluster()
	if err != nil {
		return err
	}
	c.NumberOfBrokers = len(brokers) + 1
	log.Printf("Number of brokers:  %d", c.NumberOfBrokers)
	return nil
}
