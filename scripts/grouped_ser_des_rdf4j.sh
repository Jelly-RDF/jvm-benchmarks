#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.runGroupedSerDesRdf4jBench"
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
  "triples openaire-lod"
  "triples politiquices"
  "triples yago-annotated-facts"
)

for dataset in "${DATASETS[@]}"
do
  IFS=" " read -r -a ds <<< "$dataset"
  echo "Running grouped raw ser/des RDF4J for ${ds[0]} ${ds[1]}"
  # Run with 100k elements
  $JAVA_EXEC $JAVA_OPTS -Djelly.benchmark.output-dir=./result/grouped_ser_des_rdf4j/ \
    -cp $CP "ser,des" "${ds[0]}" 0 100000 "$BASE_DATA/${ds[1]}.jelly.gz"
done
