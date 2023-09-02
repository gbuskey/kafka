package main

import (
	"context"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	log.Default().Println("starting producer...")

	seeds := []string{"localhost:9092"}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: "re.polaris", Value: []byte("match")}
	cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			log.Default().Printf("record had a produce error: %v\n", err)
		}
		log.Default().Println("produced message!")

	})
	wg.Wait()
	log.Default().Println("terminating producer...")
}
