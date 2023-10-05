#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.StreamSerDesBench"
BASE_DATA=$3

JAVA_OPTS="-Xms1G -Xmx32G"

TASKS="ser des"
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
  for task in $TASKS
  do
    IFS=" " read -r -a ds <<< "$dataset"
    echo "Running $task for ${ds[0]} ${ds[1]}"
    $JAVA_EXEC $JAVA_OPTS -Djelly.debug.output-dir=./result/stream_ser_des -cp $CP "$task" "${ds[0]}" "$BASE_DATA/${ds[1]}.jelly.gz"
  done
done
