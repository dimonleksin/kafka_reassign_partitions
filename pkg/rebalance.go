package pkg

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cheggaaa/pb/v3"
)

// Returning list of brokers with topic.name-partitions-replicaAssigment
func (c *Cluster) GetCurrentBalance(admin sarama.ClusterAdmin, from int) (err error) {

	var counter int = 0
	fmt.Println("Start getting current assigment")
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if len(c.Brokers) == 0 {
		c.Brokers = make([]Topics, c.NumberOfBrokers)
	}

	for i := 0; i < c.NumberOfBrokers; i++ {
		c.Brokers[i].Topic = make(map[int]string)
	}
	// if --from not seted (equal -1) - geting current assign for all topics
	if from == -1 {
		for topicName, i := range topics {
			for partition, brokers := range i.ReplicaAssignment {
				for l, broker := range brokers {
					c.Brokers[broker].Topic[counter] = fmt.Sprintf("%s-%d-%d", topicName, partition, l+1)
					if l+1 == 1 {
						c.Brokers[broker].Leaders += 1
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
	fmt.Println("# # # # # # # # Current assign # # # # # # # #")
	for i, v := range c.Brokers {
		fmt.Printf("Before rebalance inside broker %d contains %d partitions\n", i, len(v.Topic))
		fmt.Printf("\tFor broker %d before rebalance number by leaders %d\n\n", i, v.Leaders)
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

func (c Cluster) Rebalance(admin sarama.ClusterAdmin, numberOfTopics int, Treads int) (err error) {
	var (
		counter int
		wg      sync.WaitGroup
		errors  []error
	)
	fmt.Println()
	fmt.Println("Starting rebalance...")
	plane, err := c.ExtructPlane(numberOfTopics)
	if err != nil {
		return err
	}
	counter = len(plane)
	bar := pb.StartNew(counter)

	// errCh := make(chan error)
	topicCh := make(chan string, Treads*2)
	newAssignCh := make(chan [][]int32, Treads*2)

	fmt.Printf("Start time: %v\n", time.Now())

	wg.Add(Treads)
	for i := 0; i < Treads; i++ {
		go reassign(admin, topicCh, newAssignCh, bar, &wg)
	}
	for k, v := range plane {
		topicCh <- k
		newAssignCh <- v
	}
	// Closing channels, because read from closed channels is posible
	close(topicCh)
	close(newAssignCh)

	wg.Wait()
	bar.Finish()
	fmt.Println(errors)
	fmt.Printf("End time: %v", time.Now())
	return nil
}

func reassign(
	admin sarama.ClusterAdmin,
	topic chan string,
	newAssign chan [][]int32,
	bar *pb.ProgressBar,
	wg *sync.WaitGroup,
) {
	for t := range topic {
		err := admin.AlterPartitionReassignments(t, <-newAssign)
		if err != nil {
			fmt.Println(err)
		}
		bar.Increment()
	}
	wg.Done()
}
