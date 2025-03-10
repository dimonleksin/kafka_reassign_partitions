package pkg

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

func (c *Cluster) Restore() {
	b := backup.Backup{}
	b.GetBackup()
	c.CopyBackupToCluster(b)
}

func (c *Cluster) CopyBackupToCluster(b backup.Backup) {
	if len(c.Brokers) == 0 {
		c.Brokers = make([]Topics, c.NumberOfBrokers)
	}
	for i := 0; i < len(b.Brokers); i++ {
		if len(c.Brokers[i].Topic) == 0 {
			c.Brokers[i].Topic = make(map[int]string)
		}
		c.Brokers[i].Topic = b.Brokers[i].Topic
		c.Brokers[i].Leaders = b.Brokers[i].Leaders
	}
	c.NumberOfBrokers = b.NumberOfBrokers
}

func (c *Cluster) CopyClusterToBackup(b *backup.Backup) {
	if len(b.Brokers) == 0 {
		b.Brokers = make([]backup.Topic, c.NumberOfBrokers)
	}
	for i := 0; i < len(c.Brokers); i++ {
		if len(b.Brokers[i].Topic) == 0 {
			b.Brokers[i].Topic = make(map[int]string)
		}
		b.Brokers[i].Topic = c.Brokers[i].Topic
		b.Brokers[i].Leaders = c.Brokers[i].Leaders
	}
	b.NumberOfBrokers = c.NumberOfBrokers
}
