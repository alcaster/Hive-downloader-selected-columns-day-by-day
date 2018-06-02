package main

import (
	"time"
	"strings"
	"fmt"
	"os/exec"
	"bytes"
	"sync"
	log "github.com/sirupsen/logrus"
)

func extractCsvs(host string, columnsArrays [] string, done chan<- string, startDate time.Time, endDate time.Time, tablename string, wg sync.WaitGroup) () {
	currentDate := startDate
	for currentDate.Before(endDate) {
		dateStr := currentDate.Format("2006-01-02")
		err, filename := getSingleCsvOnServer(host, dateStr, columnsArrays, tablename)
		if err == nil {
			done <- filename
		}
		currentDate = currentDate.AddDate(0, 0, 1)
	}
	done <- "done"
	wg.Done()
}

func getSingleCsvOnServer(host string, dateStr string, columns []string, tablename string) (error, string) {
	columnsStr := strings.Join(columns, ",")
	filename := fmt.Sprintf("/tmp/%s___%s.csv", tablename, dateStr)
	command := fmt.Sprintf(`hive -e 'SELECT %s `+
		`FROM %s where day="%s";'`+
		`| sed 's/[\t]/,/g' > %s`, columnsStr, tablename, dateStr, filename)
	log.Info("Extracting csv: " + filename)
	log.Debug("Command: " + command)
	cmd := exec.Command("ssh", host, command)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Errorf("Didn't extracted %s, error:%v", filename, err)
	}
	log.Info("Successfully extracted csv: " + filename)
	return err, filename
}
