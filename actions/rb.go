package actions

import (
	"fmt"
	"log"

	"github.com/dimonleksin/kafka_reasign_partition/pkg"
)

func Reasign(settings pkg.Settings) {
	var (
		err            error
		RebalancePlane pkg.Cluster
		numberOfTopics int
		l              int
		responce       string
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
	err = r.GetCurrentBalance(admin, *settings.From)
	if err != nil {
		log.Fatal(err)
	}
	if len(settings.To) == 0 {
		RebalancePlane, err = r.CreateRebalancePlane(nil)
	} else {
		RebalancePlane, err = r.CreateRebalancePlane(settings.To)
	}
	if err != nil {
		log.Fatal(err)
	}
	r = RebalancePlane

	fmt.Println()

	fmt.Println("# # # # # # # Assign after rebalance # # # # # # #")
	for i, v := range r.Brokers {
		l = len(v.Topic)
		numberOfTopics += l
		fmt.Printf("After rebalance inside broker %d contains %d topics\n", i, l)
		fmt.Printf("\tFor broker %d after rebalance number by leaders %d\n\n", i, v.Leaders)
	}
	fmt.Println("# # # # # # # # # # # # # # # # # #  # # # # # # #")
	fmt.Print("\n\nPlane to reassign. Are you sure?[y/n]: ")
	_, err = fmt.Scan(&responce)
	if err != nil {
		log.Fatal("Error read you responce", err)
	}

	if responce == "y" {
		err = r.Rebalance(admin, numberOfTopics, *settings.Treads)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Print("So, goodby")
	}

}
