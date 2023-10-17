#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.GrpcLatencyBench"
BASE_DATA=$3
PORT=$4

JAVA_OPTS="-Xms1G -Xmx32G"

GZIP_OPTS="0 1"
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
  "triples politiquices"
  "triples yago-annotated-facts"
)

for dataset in "${DATASETS[@]}"
do
  for gzip_opt in $GZIP_OPTS
  do
    IFS=" " read -r -a ds <<< "$dataset"
    echo "Running element size gzip $gzip_opt for ${ds[0]} ${ds[1]}"
    $JAVA_EXEC $JAVA_OPTS \
      -Djelly.debug.output-dir=./result/grpc_latency/ \
      -Dpekko.grpc.client.jelly-rdf-client.port=$PORT \
      -cp $CP "$gzip_opt" "${ds[0]}" "$BASE_DATA/${ds[1]}.jelly.gz"
  done
done
