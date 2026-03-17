package backup

type Backup struct {
	Brokers         map[int32]Topics `json:"brokers"`
	NumberOfBrokers int32
}
type Topics struct {
	Topic   map[int]string `json:"topic"`
	Leaders int
}
