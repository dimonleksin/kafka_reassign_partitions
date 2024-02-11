package actions

import (
	"fmt"
	"log"

	"github.com/dimonleksin/kafka_reasign_partition/pkg"
)

func MoveTopic(settings pkg.Settings) (err error) {
	var (
		responce       string
		numberOfTopics int
		l              int
	)
	admin, err := settings.Conf()
	if err != nil {
		log.Fatal(err)
	}

	defer admin.Close()
	r := pkg.Cluster{}
	err = r.GetNumberOfBrokers(admin)

	if err != nil {
		log.Fatal(err)
	}
	if r.NumberOfBrokers < len(settings.To) {
		return fmt.Errorf("current number of brokers(%d) > value of --to (%d)", r.NumberOfBrokers, len(settings.To))
	}
	err = r.DescribeTopic(admin, settings.Topics)
	if err != nil {
		log.Fatal(err)
	}
	plane, err := r.CreateRebalancePlane(settings.To)
	if err != nil {
		log.Fatal(err)
	}
	r = plane
	for i, v := range r.Brokers {
		// for _, t := range v.Topic {
		// 	log.Printf("After rebalance inside broker %d contains %v", i, t)
		// }
		l = len(v.Topic)
		numberOfTopics += l
		log.Printf("After rebalance inside broker %d contains %d topics", i, l)
		log.Printf("For broker %d after rebalance number by leaders %d", i, v.Leaders)
		fmt.Println()
	}

	fmt.Print("\n\nPlane to reassign. Are you sure?[y/n]: ")
	_, err = fmt.Scan(&responce)
	if err != nil {
		log.Fatal("Error read you responce")
	}

	if responce == "y" {
		err = r.Rebalance(admin, numberOfTopics)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Print("So, goodby")
	}
	return nil
}
