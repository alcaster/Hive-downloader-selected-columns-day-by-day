package main

import (
	"os/exec"
	"fmt"
	"bytes"
	"log"
	"sync"
)

func downloaderCsv(host string, done <-chan string, to string, wg sync.WaitGroup) () {
	for {
		csvFilename := <-done
		if csvFilename == "done" {
			break
		}
		downloadSingleCsv(host, csvFilename, to)
		deleteRemoteCsv(host, csvFilename)
	}
	wg.Done()
}
func downloadSingleCsv(host string, csvFilename string, to string) () {
	file := fmt.Sprintf("%s:%s", host, csvFilename)
	fmt.Printf("Downloading %s, to %s", file, to)
	cmd := exec.Command("scp", file, to)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Succesfully downloaded csv: " + csvFilename)

}
func deleteRemoteCsv(host string, csvFilename string) () {
	fmt.Println("Deleting ", csvFilename)
	command := fmt.Sprintf("rm %s", csvFilename)
	cmd := exec.Command("ssh", host, command)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Succesfully removed csv %s from the server\n", csvFilename)
}
