package main

import (
	"flag"
	"strings"
	"fmt"
	"time"
	"sync"
)

func main() {
	host, columnsArrays, startDate, endDate, tablename, to := argParser()

	fmt.Printf("Getting columns:%s, from:%v, starting:%v, to:%v\n", columnsArrays, host, startDate, endDate)
	done := make(chan string, 5)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go extractCsvs(host, columnsArrays, done, startDate, endDate, tablename, *wg)
	wg.Add(1)
	go downloaderCsv(host, done, to, *wg)
	wg.Wait()
	fmt.Println("All done")
}

func argParser() (string, []string, time.Time, time.Time, string, string) {
	host := flag.String("host", "HOST", "format - user@host")
	columns := flag.String("columns", "time lat lon delay", "list of columns separated by space")
	tablename := flag.String("tablename", "buses_clean_with_timetables_archived", "name of the table")
	to := flag.String("to", "DIR", "Directory where downloaded files should be stored")
	startDateStr := flag.String("start date", "1.09.2017", "from when download")
	endDateStr := flag.String("end date", "1.11.2017", "to when download")

	layout := "2.01.2006"
	startDate, _ := time.Parse(layout, *startDateStr)
	endDate, _ := time.Parse(layout, *endDateStr)
	columnsArrays := strings.Split(*columns, " ")
	flag.Parse()
	return *host, columnsArrays, startDate, endDate, *tablename, *to
}
