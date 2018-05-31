package main

import (
	"time"
	"strings"
	"fmt"
	"os/exec"
	"bytes"
	"log"
	"sync"
)

func extractCsvs(host string, columnsArrays [] string, done chan<- string, startDate time.Time, endDate time.Time, tablename string, wg sync.WaitGroup) () {
	currentDate := startDate
	for currentDate.Before(endDate) {
		dateStr := currentDate.Format("2006-01-02")
		filename := getSingleCsvOnServer(host, dateStr, columnsArrays, tablename)
		done <- filename
		currentDate = currentDate.AddDate(0, 0, 1)
	}
	done <- "done"
	wg.Done()
}

func getSingleCsvOnServer(host string, dateStr string, columns []string, tablename string) (string) {
	columnsStr := strings.Join(columns, ",")
	filename := fmt.Sprintf("/tmp/%s___%s.csv", tablename, dateStr)
	command := fmt.Sprintf(`hive -e 'SELECT %s `+
		`FROM %s where day="%s";'`+
		`| sed 's/[\t]/,/g' > %s`, columnsStr, tablename, dateStr, filename)
	fmt.Println("Extracting csv: " + filename)
	fmt.Println("Running command ", command)
	cmd := exec.Command("ssh", host, command)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Succesfully extracted csv: " + filename)
	return filename
}
