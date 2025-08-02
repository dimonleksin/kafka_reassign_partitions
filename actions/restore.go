package actions

import (
	"fmt"
	"log"
	"os"

	"github.com/dimonleksin/kafka_reasign_partition/cmd"
	"github.com/dimonleksin/kafka_reasign_partition/internal/settings"
	"github.com/dimonleksin/kafka_reasign_partition/internal/stuff/userresponce"
)

func Restore(settings settings.Settings) {
	var (
		numberOfTopics int
		responce       string
		err            error
	)

	admin, err := settings.Conf()
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	c := cmd.Cluster{}
	err = c.GetNumberOfBrokers(admin)
	if err != nil {
		log.Fatal(err)
	}
	c.Restore(settings.MoveSetting.BackupVersion)
	RebalancePlane, numberOfTopics, err := c.CreateRebalancePlane(nil)
	if err != nil {
		fmt.Printf("Error create assign plane. %v", err)
		os.Exit(1)
	}
	fmt.Println()
	c = RebalancePlane

	fmt.Println(cmd.MakeTable(c.Brokers, "Assign after restore"))
	fmt.Print("\n\nPlane to reassign. Are you sure?[y/n]: ")
	_, err = fmt.Scan(&responce)
	if err != nil {
		fmt.Printf("Error read you responce. %v", err)
		os.Exit(1)
	}
	if responce == userresponce.YES {
		err = c.Rebalance(admin, numberOfTopics, *settings.MoveSetting.Treads, settings)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Print("So, goodby")
	}

}
