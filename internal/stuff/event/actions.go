package event

import "os"

type Actions string

const (
	REBALANCE Actions = "rebalance"
	MOVE      Actions = "move"
	RESTORE   Actions = "restore"
)

func GetEvent(e string) Actions {
	switch e {
	case string(REBALANCE):
		return REBALANCE
	case string(MOVE):
		return MOVE
	case string(RESTORE):
		return RESTORE
	}
	os.Exit(2)
	return REBALANCE
}
