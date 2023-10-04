#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.FlatSerDesBench"
BASE_DATA=$3
ELEMENT_SIZE=$4

JAVA_OPTS="-Xms1G -Xmx32G"

TASKS="ser des"
DATASETS=(
  "triples assist-iot-weather"
  "quads assist-iot-weather-graphs"
  "triples citypulse-traffic"
  "quads citypulse-traffic-graphs"
  "triples dbpedia-live"
  "triples digital-agenda-indicators"
  "triples linked-spending"
  "triples lod-katrina"
  "triples muziekweb"
  "quads nanopubs"
  "triples politiquices"
  "triples yago-annotated-facts"
)

for dataset in "${DATASETS[@]}"
do
  for task in $TASKS
  do
    IFS=" " read -r -a ds <<< "$dataset"
    echo "Running $task for ${ds[0]} ${ds[1]}, element size: $ELEMENT_SIZE"
    $JAVA_EXEC $JAVA_OPTS -Djelly.debug.output-dir=./result/ -cp $CP "$task" "$ELEMENT_SIZE" "${ds[0]}" "$BASE_DATA/${ds[1]}.jelly.gz"
  done
done
