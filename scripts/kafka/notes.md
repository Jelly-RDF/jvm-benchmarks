Start up the Kafka cluster:

```shell
docker compose up -d
```

Create a topic (must be done before running the experiments):

```shell
docker exec --workdir /opt/kafka/bin -it broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic rdf
```

Stop the Kafka cluster:

```shell
docker compose down
```
