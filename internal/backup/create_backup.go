package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func (b Backup) CreateBackup(c interface{}) {
	file := b.createBackupFile()
	defer file.Close()
	data, err := json.Marshal(b)
	if err != nil {
		fmt.Printf("error marshaling backup to json: %v\n", err)
		os.Exit(1)
	}
	file.Write(data)
}

func (b *Backup) createBackupFile() *os.File {
	home, _ := os.UserHomeDir()
	backups := b.getOldBackups(home)
	var new_backups_file_name string
	if len(backups) > 0 {
		last_backub_version := getBackupsVersion(backups[len(backups)-1])
		new_backups_file_name = fmt.Sprintf("%s/%s/krpg_backup.v.%d", home, PathToBackup, last_backub_version+1)
	} else {
		new_backups_file_name = fmt.Sprintf("%s/%s/krpg_backup.v.1", home, PathToBackup)
	}
	file, err := os.Create(new_backups_file_name)
	if err != nil {
		fmt.Printf("error create backup file: %v\n", err)
		os.Exit(1)
	}
	return file
}

func getBackupsVersion(name string) (version_of_backup int) {
	v := strings.Split(name, ".")
	int_ver := v[len(v)-1]

	version_of_backup, _ = strconv.Atoi(int_ver)
	return version_of_backup
}
