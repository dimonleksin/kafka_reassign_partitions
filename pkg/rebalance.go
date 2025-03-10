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
	fmt.Println(MakeTable(c.Brokers))

	return nil
}

func (c Cluster) CreateRebalancePlane(to []int) (result Cluster, numberOfTopics int, err error) {
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
		return result, 0, err
	}
	numberOfTopics = len(allTopics)
	result, err = makePlane(allTopicsSort, c.NumberOfBrokers, to)

	if err != nil {
		return result, numberOfTopics, nil
	}
	return result, numberOfTopics, nil
}

func (c Cluster) Rebalance(admin sarama.ClusterAdmin, numberOfTopics int, Treads int, settings Settings) (err error) {
	var (
		counter int
		wg      sync.WaitGroup
		errors  []error
		bar     *pb.ProgressBar
	)
	fmt.Println()
	fmt.Println("Starting rebalance...")
	plane, err := c.ExtructPlane(numberOfTopics)
	if err != nil {
		return err
	}
	counter = len(plane)
	if settings.Verbose {
		bar = nil
	} else {
		bar = pb.StartNew(counter)
	}

	// errCh := make(chan error)
	topicCh := make(chan string, Treads*2)
	newAssignCh := make(chan [][]int32, Treads*2)

	fmt.Printf("Start time: %v\n", time.Now())

	wg.Add(Treads)
	for i := 0; i < Treads; i++ {
		go reassign(admin, topicCh, newAssignCh, settings, bar, &wg)
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

/*
generate request for reassign topic,
and if --sync then wait result
*/
func reassign(
	admin sarama.ClusterAdmin,
	topic chan string,
	newAssign chan [][]int32,
	settings Settings,
	bar *pb.ProgressBar,
	wg *sync.WaitGroup,
) {
	for t := range topic {
		partitionsWithAssign := <-newAssign
		listOfPartitions := make([]int32, len(partitionsWithAssign))
		for k, _ := range partitionsWithAssign {
			listOfPartitions = append(listOfPartitions, int32(k))
		}
		err := admin.AlterPartitionReassignments(t, partitionsWithAssign)
		if settings.Verbose {
			fmt.Printf("reassign topic %s\n", t)
		}
		if err != nil {
			fmt.Println(err)
		}
		if !settings.MoveSetting.Sync {
			if settings.Verbose {
				fmt.Printf("wait for topic %s\n", t)
			}
			for {
				status, err := admin.ListPartitionReassignments(t, listOfPartitions)
				if err != nil {
					fmt.Println(err)
				}
				statusOfTopic := status[t]
				for partitions, partitionStatus := range statusOfTopic {
					/*
						if in AddingReplicas and RemovingReplicas here are none left elements
						then all replicas for this partition moved
					*/
					if len(partitionStatus.AddingReplicas) == 0 && len(partitionStatus.RemovingReplicas) == 0 {
						delete(statusOfTopic, partitions)
					}
				}
				/*
					if in statusOfTopic there are none left elements
					then all partitions inbalanced
				*/
				if len(statusOfTopic) == 0 {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if settings.Verbose {
				fmt.Printf("all partitions for topic %s succesfule reassigned\n", t)
			}
		}
		if !settings.Verbose {
			bar.Increment()
		}
	}
	wg.Done()
}
