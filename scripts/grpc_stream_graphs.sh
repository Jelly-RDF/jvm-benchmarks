#!/bin/bash

set -eux

JAVA_EXEC=$1
CP="$2 eu.ostrzyciel.jelly.benchmark.GrpcStreamBench"
BASE_DATA=$3
PORT=$4

JAVA_OPTS="-Xms1G -Xmx32G"

GZIP_OPTS="0 1"
DATASETS=(
  "graphs assist-iot-weather-graphs"
  "graphs citypulse-traffic-graphs"
  "graphs nanopubs"
)
ELEMENTS="0 1024 4096"

for dataset in "${DATASETS[@]}"
do
  for gzip_opt in $GZIP_OPTS
  do
    for el in $ELEMENTS
    do
      IFS=" " read -r -a ds <<< "$dataset"
      echo "Running element size $el gzip $gzip_opt for ${ds[0]} ${ds[1]}"
      $JAVA_EXEC $JAVA_OPTS \
        -Djelly.debug.output-dir=./result/grpc_stream/ \
        -Dpekko.grpc.client.jelly-rdf-client.port=$PORT \
        -cp $CP "$gzip_opt" "${ds[0]}" "$el" "$BASE_DATA/${ds[1]}.jelly.gz"
    done
  done
done
