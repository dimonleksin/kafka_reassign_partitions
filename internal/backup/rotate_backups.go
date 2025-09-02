package backup

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
)

const PathToBackup string = ".krpg"

func (b Backup) getOldBackups(home string) []string {
	backup_path := fmt.Sprintf("%v/%v", home, PathToBackup)
	backups, err := os.ReadDir(backup_path)
	if err != nil {
		log.Fatalf("error get backup dir. err: %v", err)
	}
	return b.sortBackups(backups)
}

func (b Backup) sortBackups(backups []fs.DirEntry) []string {
	sortedFiles := make([]string, len(backups)+1)
	for i := 0; i < len(backups); i++ {
		v := getBackupsVersion(backups[i].Name())
		sortedFiles[v] = backups[i].Name()
	}
	return sortedFiles
}

func (b Backup) createBackupDir(home string) {
	if isBackupExist(home) {
		return
	}
	backupDir := fmt.Sprintf("%s/%s", home, PathToBackup)
	err := os.Mkdir(backupDir, os.ModePerm)
	if err != nil {
		fmt.Printf("error create backup dir. err: %v", err.Error())
		os.Exit(1)
	}
}

// return true if dir for backup alredy exist
func isBackupExist(home string) bool {
	dirs, err := os.ReadDir(home)
	if err != nil {
		fmt.Printf("error get user home dir. err: %v", err.Error())
		os.Exit(1)
	}
	for _, dir := range dirs {
		if dir.Type().IsDir() && dir.Name() == PathToBackup {
			return true
		}
	}
	return false
}

func changeBackupVersion(backup_file_1, backup_file_2 string) error {
	backup_old_file, err := os.OpenFile(backup_file_1, os.O_RDWR, os.ModeAppend)
	if err != nil {
		return err
	}
	defer backup_old_file.Close()
	err = os.Remove(backup_file_2)
	if err != nil {
		return err
	}
	backup_new_file, err := os.Create(backup_file_2)
	if err != nil {
		return err
	}
	_, err = io.Copy(backup_new_file, backup_old_file)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backup) MoveOldBackups() {
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(2)
	}
	b.createBackupDir(home)
	backups := b.getOldBackups(home)
	if len(backups) < 10 {
		return
	}

	backup_path := fmt.Sprintf("%v/%v", home, PathToBackup)
	for i := 1; i < len(backups)-1; i++ {
		new_ver := fmt.Sprintf("%v/%v", backup_path, backups[i])
		old_ver := fmt.Sprintf("%v/%v", backup_path, backups[i+1])
		err = changeBackupVersion(old_ver, new_ver)
		if err != nil {
			fmt.Printf("error change backup version. Err: %v", err.Error())
			os.Exit(1)
		}
	}
	last_backup := fmt.Sprintf("%v/%v", backup_path, backups[len(backups)-1])
	fmt.Println(last_backup)
	os.Remove(last_backup)
}
