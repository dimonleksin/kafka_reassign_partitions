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

// Shufle current broker in broker list from --to for uniform reasign
func shufleCounter(to []int) (newI []int) {
	tmp := to[0]
	to[0] = to[1]
	to[1] = to[2]
	to[2] = tmp
	return to
}

// Maked plane for rebalance | nob - Number Of Brokers
func makePlane(topics map[int]string, nob int, to []int) (result Cluster, err error) {
	counter := 1
	if to != nil {
		counter = to[0]
	}

	if len(result.Brokers) == 0 {
		result.Brokers = make([]Topics, nob)
	}
	// allocating memory for all brokers in map
	for i := 0; i < nob; i++ {
		result.Brokers[i].Topic = make(map[int]string)
	}

	for i := 0; i < len(topics); i++ {
		if counter == nob {
			counter = 1
		}
		if to != nil {
			counter = to[0]
		}

		// Getting role from topic: 1 - leader, 2 and other - replicas
		currentRole, err := strconv.Atoi(strings.Split(topics[i], "-")[len(strings.Split(topics[i], "-"))-1])
		if err != nil {
			return result, err
		}
		for search(result.Brokers[counter].Topic, topics[i][0:len(topics[i])-2]) {
			// if --to not set, reasign for all brokers
			if to == nil {
				counter++
			} else {
				// if --to seted, reasign to brokers from --to
				counter = to[0]
				to = shufleCounter(to)
			}
			if counter == nob {
				counter = 1
			}
		}

		if currentRole == 1 {
			result.Brokers[counter].Leaders += 1
		}

		result.Brokers[counter].Topic[i] = topics[i]
		if to == nil {
			counter++
		} else {
			counter = to[0]
			to = shufleCounter(to)
		}
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
