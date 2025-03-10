package actions

import (
	"fmt"
	"log"
	"os"

	"github.com/dimonleksin/kafka_reasign_partition/pkg"
)

func Restore(settings pkg.Settings) {
	var (
		err            error
		numberOfTopics int
		responce       string
	)

	admin, err := settings.Conf()
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	c := pkg.Cluster{}
	err = c.GetNumberOfBrokers(admin)
	if err != nil {
		log.Fatal(err)
	}
	c.Restore()
	RebalancePlane, numberOfTopics, err := c.CreateRebalancePlane(nil)
	if err != nil {
		fmt.Printf("Error create assign plane. %v", err)
		os.Exit(1)
	}
	fmt.Println()
	c = RebalancePlane

	fmt.Println("# # # # # # # Assign after restore # # # # # # #")
	fmt.Println(pkg.MakeTable(c.Brokers))
	fmt.Println("# # # # # # # # # # # #  # # # # # # # # # # # #")
	fmt.Print("\n\nPlane to reassign. Are you sure?[y/n]: ")
	_, err = fmt.Scan(&responce)
	if err != nil {
		fmt.Printf("Error read you responce. %v", err)
		os.Exit(1)
	}
	if responce == "y" {
		err = c.Rebalance(admin, numberOfTopics, *settings.MoveSetting.Treads, settings)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Print("So, goodby")
	}

}
