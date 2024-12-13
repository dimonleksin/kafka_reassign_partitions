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
	if r.NumberOfBrokers < len(settings.MoveSetting.To) {
		return fmt.Errorf("current number of brokers(%d) > value of --to (%d)", r.NumberOfBrokers, len(settings.MoveSetting.To))
	}
	err = r.DescribeTopic(admin, settings.MoveSetting.Topics)
	if err != nil {
		log.Fatal(err)
	}
	plane, err := r.CreateRebalancePlane(settings.MoveSetting.To)
	if err != nil {
		log.Fatal(err)
	}
	r = plane
	for i, v := range r.Brokers {
		l = len(v.Topic)
		numberOfTopics += l
		fmt.Printf("\nAfter rebalance inside broker %d contains %d topics\n", i, l)
		fmt.Printf("\tFor broker %d after rebalance number by leaders %d\n\n", i, v.Leaders)
	}

	fmt.Print("\n\nPlane to reassign. Are you sure?[y/n]: ")
	_, err = fmt.Scan(&responce)
	if err != nil {
		log.Fatal("Error read you responce")
	}

	if responce == "y" {
		err = r.Rebalance(admin, numberOfTopics, *settings.MoveSetting.Treads)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Print("So, goodby")
	}
	return nil
}
