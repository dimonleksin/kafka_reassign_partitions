package main

import (
	"log"
	"os"

	"github.com/dimonleksin/kafka_reasign_partition/actions"
	"github.com/dimonleksin/kafka_reasign_partition/internal/settings"
	"github.com/dimonleksin/kafka_reasign_partition/internal/stuff"
	"github.com/dimonleksin/kafka_reasign_partition/internal/stuff/event"
)

func main() {
	settings := settings.Settings{}
	err := settings.GetSettings()
	if err != nil {
		log.Fatal(err)
	}
	if *settings.H || *settings.Help {
		stuff.PrintHelp()
		os.Exit(0)
	}
	if settings.MoveSetting.Action == event.REBALANCE || settings.MoveSetting.Action == event.MOVE && *settings.MoveSetting.From != -1 {
		actions.Reasign(settings)
	} else if settings.MoveSetting.Action == event.MOVE && *settings.MoveSetting.From == -1 {
		if len(settings.MoveSetting.Topics) != 0 {
			err := actions.MoveTopic(settings)
			if err != nil {
				log.Fatal(err)
			}
		}
	} else if settings.MoveSetting.Action == event.RESTORE {
		actions.Restore(settings)
	}
}
