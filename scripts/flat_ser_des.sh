#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.runFlatSerDesBench"
BASE_DATA=$3

JAVA_OPTS="-Xms1G -Xmx32G"

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
  "triples openaire-lod"
  "triples politiquices"
  "triples yago-annotated-facts"
)

for dataset in "${DATASETS[@]}"
do
  IFS=" " read -r -a ds <<< "$dataset"
  echo "Running flat raw ser/des for ${ds[0]} ${ds[1]}"
  # Run with 5 million statements
  $JAVA_EXEC $JAVA_OPTS -Djelly.benchmark.output-dir=./result/flat_ser_des/ \
    -cp $CP "ser,des" "${ds[0]}" 512 5000000 "$BASE_DATA/${ds[1]}.jelly.gz"
done
