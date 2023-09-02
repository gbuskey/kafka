# kafka

Docs: https://docs.redpanda.com/22.1/quickstart/quick-start-docker/#single-command-for-a-1-node-cluster

Start red-panda

```bash
docker run -d --pull=always --name=redpanda-1 --rm \
-p 8081:8081 \
-p 8082:8082 \
-p 9092:9092 \
-p 9644:9644 \
docker.redpanda.com/redpandadata/redpanda:latest \
redpanda start \
--overprovisioned \
--smp 1  \
--memory 1G \
--reserve-memory 0M \
--node-id 0 \
--check=false
```

Create topic. Only needed for initial setup

```
 docker exec -it redpanda-1 \
 rpk topic create re.polaris --brokers=localhost:9092
```

Begin consuming messages

```
cd sdk
go run main.go
```

Produce a message
```
cd producer
go run main.go
```

Message should appear in the consumer terminal

```
2023/09/01 22:11:19 I recieved a task with value 'match'
```