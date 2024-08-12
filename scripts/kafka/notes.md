Start up the Kafka cluster:

```shell
docker compose up -d
```

Create a topic (optional, Kafka will auto-create topics if they don't exist):

```shell
docker exec --workdir /opt/kafka/bin -it broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic rdf
```

Delete a topic:

```shell
docker exec --workdir /opt/kafka/bin -it broker ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic rdf
```

Stop the Kafka cluster:

```shell
docker compose down
```
