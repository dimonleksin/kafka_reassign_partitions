package pkg

import (
	"github.com/jedib0t/go-pretty/v6/table"
)

// make table for pretty print brokers with partitions
func MakeTable(topics []Topics) string {
	t := table.NewWriter()
	total := 0
	t.AppendHeader(table.Row{
		"Broker ID",
		"Leaders Sum",
		"Partitions Sum",
	})

	t.SetAutoIndex(true)

	for index, row := range topics {
		if len(row.Topic) > 0 {
			t.AppendRow(table.Row{
				index,
				row.Leaders,
				len(row.Topic),
			})
			total += len(row.Topic)
		}
	}
	t.AppendFooter(table.Row{"TOTAL", "", total})
	return t.Render()
}
