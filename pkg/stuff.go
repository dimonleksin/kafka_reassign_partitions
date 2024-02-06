package pkg

import (
	"strconv"
	"strings"
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
func makePlane(topics map[int]string) (result Cluster, err error) {
	counter := 1
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
		if currentRole == 1 {
			result.Brokers[counter].Leaders += 1
		}

		result.Brokers[counter].Topic[i] = topics[i]
		counter++
	}

	return result, nil
}
