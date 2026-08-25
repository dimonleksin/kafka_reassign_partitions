package backup

// Backup represents saved cluster assignment. Brokers is a slice of Topic
// entries that include the real BrokerID so backups don't rely on array
// positions matching broker IDs.
type Backup struct {
	Brokers         []Topic `json:"brokers"`
	NumberOfBrokers int     `json:"number_of_brokers"`
}

type Topic struct {
	BrokerID int            `json:"broker_id"`
	Topic    map[int]string `json:"topic"`
	Leaders  int            `json:"leaders"`
}
