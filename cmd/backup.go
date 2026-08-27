package cmd

import (
	"github.com/dimonleksin/kafka_reasign_partition/internal/backup"
)

// create backup to file
func (c Cluster) CreateBackup() {
	b := backup.Backup{}
	b.MoveOldBackups()
	c.CopyClusterToBackup(&b)
	b.CreateBackup(c)
}

func (c *Cluster) Restore(version int) {
	b := backup.Backup{}
	b.GetBackup(version)
	c.CopyBackupToCluster(b)
}

func (c *Cluster) CopyBackupToCluster(b backup.Backup) {
	if len(c.Brokers) == 0 {
		c.Brokers = make(map[int]Topics)
	}
	for _, bt := range b.Brokers {
		id := bt.BrokerID
		t := c.Brokers[id]
		if t.Topic == nil {
			t.Topic = make(map[int]string)
		}
		t.Topic = bt.Topic
		t.Leaders = bt.Leaders
		c.Brokers[id] = t
	}
	c.NumberOfBrokers = b.NumberOfBrokers
}

func (c *Cluster) CopyClusterToBackup(b *backup.Backup) {
	// Build Brokers slice with explicit BrokerID entries
	b.Brokers = make(map[int]backup.Topic)
	for id, t := range c.Brokers {
		bt := backup.Topic{
			BrokerID: id,
			Topic:    t.Topic,
			Leaders:  t.Leaders,
		}
		b.Brokers[id] = bt
	}
	b.NumberOfBrokers = c.NumberOfBrokers
}
