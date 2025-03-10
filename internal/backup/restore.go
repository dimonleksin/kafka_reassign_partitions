package backup

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func (b *Backup) GetBackup() {
	file := b.getLastBackupFile()
	by, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("error read backup file: %v", err)
		os.Exit(1)
	}
	err = json.Unmarshal(by, b)
	if err != nil {
		fmt.Printf("error unmarshal backup file: %v", err)
		os.Exit(1)
	}
}

func (b Backup) getLastBackupFile() *os.File {
	home, _ := os.UserHomeDir()

	backups := b.getOldBackups(home)
	if len(backups) > 0 {
		last_backub_version := getBackupsVersion(backups[len(backups)-1])
		backups_file_name := fmt.Sprintf("%s/%s/krpg_backup.v.%d", home, PathToBackup, last_backub_version)
		file, err := os.OpenFile(backups_file_name, os.O_RDONLY, os.ModeAppend)
		if err != nil {
			fmt.Printf("backup files not opened, err: %v", err)
			os.Exit(1)
		}
		return file
	} else {
		fmt.Printf("backup files not exist")
		os.Exit(1)
	}
	return nil
}
