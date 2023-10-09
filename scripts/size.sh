#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.SizeBench"
BASE_DATA=$3

JAVA_OPTS="-Xms1G -Xmx32G"

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
ELEMENTS="0 1024 4096"

for dataset in "${DATASETS[@]}"
do
  for el in $ELEMENTS
  do
    IFS=" " read -r -a ds <<< "$dataset"
    echo "Running element size $el for ${ds[0]} ${ds[1]}"
    $JAVA_EXEC $JAVA_OPTS -Djelly.debug.output-dir=./result/size/ -cp $CP "${ds[0]}" "$el" "$BASE_DATA/${ds[1]}.jelly.gz"
  done
done

