package main

import (
	"os/exec"
	"fmt"
	"bytes"
	"sync"
	log "github.com/sirupsen/logrus"
)

func downloaderCsv(host string, done <-chan string, to string, wg sync.WaitGroup) () {
	for {
		csvFilename := <-done
		log.Debug("Received:" + csvFilename)
		if csvFilename == "done" {
			break
		}
		downloadSingleCsv(host, csvFilename, to)
		deleteRemoteCsv(host, csvFilename)
	}
    log.Info("Downloader finished")
	wg.Done()
}
func downloadSingleCsv(host string, csvFilename string, to string) () {
	file := fmt.Sprintf("%s:%s", host, csvFilename)
	log.Infof("Downloading %s, to %s", file, to)
	cmd := exec.Command("scp", file, to)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Errorf("Didn't downloaded %s, error: %v", file, err)
		return
	}
	fmt.Println("Succesfully downloaded csv: " + csvFilename)

}
func deleteRemoteCsv(host string, csvFilename string) () {
	log.Info("Deleting ", csvFilename)
	command := fmt.Sprintf("rm %s", csvFilename)
	cmd := exec.Command("ssh", host, command)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Errorf("Didn't removed %s, error: %v", csvFilename, err)
		return
	}
	log.Infof("Successfully removed csv %s from the server\n", csvFilename)
}
