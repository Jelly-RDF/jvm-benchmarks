#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.runKafkaThroughputBench"
BASE_DATA=$3
PORT=$4

JAVA_OPTS="-Xms1G -Xmx32G"

GZIP_OPTS="false true"
DATASETS=(
  "triples assist-iot-weather"
  "quads assist-iot-weather-graphs"
  "graphs assist-iot-weather-graphs"
  "triples citypulse-traffic"
  "quads citypulse-traffic-graphs"
  "graphs citypulse-traffic-graphs"
  "triples dbpedia-live"
  "triples digital-agenda-indicators"
  "triples linked-spending"
  "triples lod-katrina"
  "triples muziekweb"
  "quads nanopubs"
  "graphs nanopubs"
  "triples openaire-lod"
  "triples politiquices"
  "triples yago-annotated-facts"
)

for dataset in "${DATASETS[@]}"
do
  for gzip_opt in $GZIP_OPTS
  do
    IFS=" " read -r -a ds <<< "$dataset"
    echo "Running gzip $gzip_opt for ${ds[0]} ${ds[1]}"
    $JAVA_EXEC $JAVA_OPTS \
      -Djelly.benchmark.output-dir=./result/kafka_throughput/ \
      -Dpekko.kafka.producer.kafka-clients.bootstrap.servers="127.0.0.1:$PORT" \
      -Dpekko.kafka.consumer.kafka-clients.bootstrap.servers="127.0.0.1:$PORT" \
      -cp $CP "$gzip_opt" "${ds[0]}" 10000 "$BASE_DATA/${ds[1]}.jelly.gz"
  done
done
