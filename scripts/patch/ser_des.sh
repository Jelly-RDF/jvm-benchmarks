#!/bin/bash

set -eux

JAVA_EXEC=$1
SBT_HOME=$2
BASE_DATA=$3

JAVA_OPTS="-Xms1G -Xmx32G"

DATASETS=(
  "triples assist-iot-weather"
  "triples bsbm-cdc"
)

for dataset in "${DATASETS[@]}"
do
  IFS=" " read -r -a ds <<< "$dataset"
  echo "Running serialization for ${ds[0]} ${ds[1]}"

  $JAVA_EXEC -jar "$SBT_HOME/sbt-launch.jar" \
    "Jmh/run -f7 -wi 10 -i 20 -rf json -rff patch_ser_$(date --iso-8601=seconds).json --jvmArgs \"$JAVA_OPTS -Djelly.patch.input-file=$BASE_DATA/${ds[1]}.jellyp -Djelly.patch.statement-type=${ds[0]}\" .*SerBench.*"

  echo "Running deserialization for ${ds[0]} ${ds[1]}"

  $JAVA_EXEC -jar "$SBT_HOME/sbt-launch.jar" \
   "Jmh/run -f7 -wi 10 -i 20 -rf json -rff patch_des_$(date --iso-8601=seconds).json --jvmArgs \"$JAVA_OPTS -Djelly.patch.input-file=$BASE_DATA/${ds[1]}.jellyp -Djelly.patch.statement-type=${ds[0]} -Djelly.patch.max-segments=250000\" .*DesBench.*"
done
