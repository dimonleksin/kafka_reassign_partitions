package main

import (
	"fmt"
	"log"

	"github.com/dimonleksin/kafka_reasign_partition/pkg"
)

func main() {
	var (
		err            error
		RebalancePlane pkg.Cluster
	)

	settings := pkg.Settings{}
	err = settings.GetSettings()
	if err != nil {
		log.Fatal(err)
	}
	switch settings.Action {
	case "move":
		admin, err := settings.Conf()
		if err != nil {
			log.Fatal(err)
		}
		r := pkg.Cluster{}
		err = r.GetCurrentBalance(admin)
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < 2; i++ {
			RebalancePlane, err = r.CreateRebalancePlane()
			if err != nil {
				log.Fatal(err)
			}
			r = RebalancePlane
		}
		fmt.Println()
		for i, v := range RebalancePlane.Brokers {
			// for _, t := range v.Topic {
			// 	log.Printf("After rebalance inside broker %d contains %v", i, t)
			// }
			log.Printf("After rebalance inside broker %d contains %d topics", i, len(v.Topic))
			log.Printf("For broker %d after rebalance number by leaders %d", i, v.Leaders)
			fmt.Println()
		}
		// log.Println(RebalancePlane)
	}
}
