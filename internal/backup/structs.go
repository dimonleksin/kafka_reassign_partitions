package backup

type Backup struct {
	Brokers         []Topic `json:"brokers"`
	NumberOfBrokers int
}
type Topic struct {
	Topic   map[int]string `json:"topic"`
	Leaders int
}
