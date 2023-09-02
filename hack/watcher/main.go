package main

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func customMatcher(record *kgo.Record) bool {
	return string(record.Value) == "match"
}

func customTaskHandler(record *kgo.Record) error {
	log.Default().Printf("I recieved a task with value '%s'", record.Value)
	return nil
}

func main() {
	log.Default().Println("starting watcher...")

	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("my-group-identifier"),
		kgo.ConsumeTopics("re.polaris"),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	// Task handler to do something with the records recieved
	taskChan := make(chan *kgo.Record, 100)
	go StartTaskHandler(taskChan, customTaskHandler)

	// Consumer will block until it recieves a shutdown signal
	ConsumeRecords(cl, customMatcher, taskChan)
}

func ConsumeRecords(cl *kgo.Client, Matches func(record *kgo.Record) bool, c chan *kgo.Record) {
	log.Default().Println("starting consumer...")

	// consume messages from a topic
	ctx := context.Background()
	for {
		log.Default().Println("fetching new batch...")
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// All errors are retried internally when fetching, but non-retriable errors are
			// returned from polls so that users can notice and take action.
			panic(fmt.Sprint(errs))
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			p.EachRecord(func(record *kgo.Record) {
				if match := Matches(record); match {
					log.Default().Println("we found a match!")
					c <- record
				}
			})
		})
	}
}

func StartTaskHandler(taskChan chan (*kgo.Record), taskHandler func(*kgo.Record) error) {
	log.Default().Println("starting task handler")

	for {
		task := <-taskChan
		if task == nil {
			log.Default().Println("task is null, leaving task handler")
			return
		}

		err := taskHandler(task)
		if err != nil {
			log.Default().Println(err)
		}
	}
}
