package actions

import (
	"fmt"
	"log"

	"github.com/dimonleksin/kafka_reasign_partition/cmd"
	"github.com/dimonleksin/kafka_reasign_partition/internal/settings"
)

func MoveTopic(settings settings.Settings) (err error) {
	var (
		responce       string
		numberOfTopics int
		plane          cmd.Cluster
	)
	admin, err := settings.Conf()
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()
	r := cmd.Cluster{}
	err = r.GetNumberOfBrokers(admin)

	if err != nil {
		log.Fatal(err)
	}
	if r.NumberOfBrokers < len(settings.MoveSetting.To) {
		return fmt.Errorf("current number of brokers(%d) < value of --to (%d)", r.NumberOfBrokers, len(settings.MoveSetting.To))
	}
	err = r.DescribeTopic(admin, settings.MoveSetting.Topics)
	if err != nil {
		log.Fatal(err)
	}
	plane, numberOfTopics, err = r.CreateRebalancePlane(settings.MoveSetting.To)
	if err != nil {
		log.Fatal(err)
	}
	r = plane

	fmt.Println(cmd.MakeTable(r.Brokers, "Assign after move"))

	fmt.Print("\n\nPlane to reassign. Are you sure?[y/n]: ")
	_, err = fmt.Scan(&responce)
	if err != nil {
		log.Fatal("Error read you responce")
	}

	if responce == "y" {
		err = r.Rebalance(admin, numberOfTopics, *settings.MoveSetting.Treads, settings)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Print("So, goodby")
	}
	return nil
}
