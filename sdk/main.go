package main

import (
	"log"
)

func main() {
	seeds := []string{"localhost:9092"}
	topics := []string{"re.polaris"}
	consumerGroup := "my-group-identifier"

	watcher, err := New(seeds, topics, consumerGroup)
	if err != nil {
		panic(err)
	}
	defer watcher.Client.Close()

	go watcher.StartTaskHandler(customTaskHandler)

	watcher.ConsumeRecords(customMatcher)
}

func customMatcher(record *Record) bool {
	return string(record.Value) == "match"
}

func customTaskHandler(record *Record) error {
	log.Default().Printf("I recieved a task with value '%s'", record.Value)
	return nil
}
