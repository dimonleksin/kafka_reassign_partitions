package pkg

import (
	"fmt"
	"strconv"
	"strings"
)

// Searching equals topics from broker and list with all topics
func search(m map[int]string, k string) bool {
	for _, v := range m {
		if k == v[0:len(v)-2] {
			return true
		}
	}
	return false
}

// return true if one of replicas contains in broker id from --from key
func searchForMove(m map[int32][]int32, key int32) bool {
	for _, v := range m {
		for i := 0; i < len(v); i++ {
			if v[i] == key {
				return true
			}
		}
	}
	return false
}

func parsTopicParams(topic string) (topicName string, partitionID, positionID int, err error) {
	tmp := strings.Split(topic, "-")
	l := len(tmp)
	// Geting topic name
	topicName = strings.Join(tmp[0:l-2], "-")
	partitionID, err = strconv.Atoi(tmp[l-2])
	if err != nil {
		return "", -1, -1, fmt.Errorf("can't parsed partition id from topic %s. Err: %v", topicName, err)
	}
	// Geting position of reasign
	positionID, err = strconv.Atoi(tmp[l-1])

	if err != nil {
		return "", -1, -1, fmt.Errorf("can't parsed position in assign id from topic %s. Err: %v", topicName, err)
	}
	return topicName, partitionID, positionID, nil
}
