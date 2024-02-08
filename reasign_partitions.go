package main

import (
	"log"

	"github.com/dimonleksin/kafka_reasign_partition/actions"
	"github.com/dimonleksin/kafka_reasign_partition/pkg"
)

func main() {
	settings := pkg.Settings{}
	err := settings.GetSettings()
	if err != nil {
		log.Fatal(err)
	}
	switch settings.Action {
	case "rebalance":
		actions.Reasign(settings)
	}
}
