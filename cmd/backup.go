package cmd

import (
	"github.com/dimonleksin/kafka_reasign_partition/internal/backup"
)

// create backup to file
func (c Cluster) CreateBackup() {
	b := backup.Backup{}
	b.Brokers = make(map[int32]backup.Topics)
	b.MoveOldBackups()
	c.CopyClusterToBackup(&b)
	b.CreateBackup(c)
}

func (c *Cluster) Restore(version int) {
	b := backup.Backup{}
	b.Brokers = make(map[int32]backup.Topics)
	b.GetBackup(version)
	c.CopyBackupToCluster(b)
}

func (c *Cluster) CopyBackupToCluster(b backup.Backup) {
	if c.Brokers == nil {
		c.Brokers = make(map[int32]Topics)
	}
	for brokerId, backupTopics := range b.Brokers {
		topic := Topics{
			Leaders: backupTopics.Leaders,
			Topic:   make(map[int]string),
		}
		for k, v := range backupTopics.Topic {
			topic.Topic[k] = v
		}
		c.Brokers[brokerId] = topic
	}
	c.NumberOfBrokers = b.NumberOfBrokers
}

func (c *Cluster) CopyClusterToBackup(b *backup.Backup) {
	if c.Brokers == nil {
		c.Brokers = make(map[int32]Topics)
	}
	for brokerId, topics := range c.Brokers {
		backupTopics := backup.Topics{
			Topic:   make(map[int]string),
			Leaders: topics.Leaders,
		}
		for k, v := range topics.Topic {
			backupTopics.Topic[k] = v
		}
		b.Brokers[brokerId] = backupTopics
	}
	b.NumberOfBrokers = c.NumberOfBrokers
}
