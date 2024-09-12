# jvm-benchmarks

Benchmarks for Jelly-JVM using Apache Jena.

**See the [Jelly website](https://w3id.org/jelly) for more information about Jelly and documentation.**

## Prerequisites

You need at least Java 17 to compile the benchmark code. To run the benchmarks, we **strongly recommend** using the latest stable version a modern JDK, preferably GraalVM. In our benchmarks we used Oracle GraalVM 22.0.2+9.

This code was tested only on Linux. It may work on other platforms, but this wasn't tested.

## Compiling

Use [sbt](https://www.scala-sbt.org/) to compile the project into a single JAR file:

```shell
sbt assembly
```

The resulting JAR will be placed in `target/scala-3.*/benchmarks-assembly-*.jar`. Use the path to the built JAR as the second argument to the benchmark scripts.

## Downloading the datasets

The benchmarks can read RDF data from gzip-compressed Jelly files (preferred) or from compressed .tar.gz files. RiverBench datasets are distributed in both of these formats ([see documentation](https://w3id.org/riverbench/v/dev/documentation/dataset-release-format)), but Jelly is much faster to read from. You can manually download the datasets, but we recommend using the built-in download utility:

```shell
java -cp [path-to-benchmark-jar] eu.ostrzyciel.jelly.benchmark.runDownloadDatasetsUtility [profile] [version] [size] [output-directory]
```

- `[profile]` is the RiverBench profile to download, for example `stream-mixed`. See a list of available profiles [here](https://w3id.org/riverbench/v/dev/categories).
- `[version]` is the version of RiverBench to use, for example `2.1.0`. We strongly recommend using the latest stable version (not `dev`!) to ensure reproducibility.
- `[size]` is the size of the datasets to download, for example: `10K`, `100K`, `1M`, `full`. If you are not sure, use `full`. The benchmark scripts can use only a part of the downloaded dataset. 
- `[output-directory]` is the directory where the datasets will be downloaded.

The script will download the datasets as `.jelly.gz` files.

## Running the benchmarks

The `scripts` directory contains scripts that automate running the benchmarks.

The scripts assume you are using the `stream-mixed` profile of RiverBench – if you are using a different profile, you will need to modify the `DATASETS` variable in the scripts accordingly. Same goes for any changes to benchmark parameters.

- `./scripts/size.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets]`
  - Runs the serialization size benchmark.
  - `[path-to-java-executable]` is the path to the Java executable to use. For example: `/usr/bin/java`.
  - `[path-to-benchmark-jar]` is the path to the compiled benchmark JAR (see: [Compiling](#compiling)).
  - `[directory-with-datasets]` is the directory with the downloaded datasets (see: [Downloading the datasets](#downloading-the-datasets)).
- `./scripts/flat_ser_des.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets]`
  - Runs the serialization/deserialization speed benchmark for flat RDF streams (streams of RDF triples or quads).
- `./scripts/grouped_ser_des.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets]`
  - Runs the serialization/deserialization speed benchmark for grouped RDF streams (streams of RDF graphs or datasets).
- `./scripts/grpc_throughput.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets] [server-port]`
  - Runs the end-to-end throughput benchmark using gRPC.
  - If you want to emulate specific network conditions for your benchmark (e.g., limited bandwidth, increased latency), see the section on network emulation below.
  - `[server-port]` is the port on which the gRPC server will listen and the gRPC client will connect to. If you want to use network emulation, set this to a port that you have configured in the network emulation script.
- `./scripts/grpc_latency.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets] [server-port]`
  - Runs the end-to-end latency benchmark using gRPC.
- `./scripts/kafka_throughput.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets] [kafka-broker-port]`
  - Runs the end-to-end throughput benchmark using Apache Kafka.
  - This script requires a running Kafka cluster. See the section on running Kafka below.
  - `[kafka-broker-port]` is the port on which the Kafka broker is listening. If you want to use network emulation, set this to a port that you have configured in the network emulation script.
- `./scripts/kafka_latency.sh [path-to-java-executable] [path-to-benchmark-jar] [directory-with-datasets] [kafka-broker-port]`
  - Runs the end-to-end latency benchmark using Apache Kafka.
  - This script requires a running Kafka cluster. See the section on running Kafka below. 

### Running Kafka

For Kafka benchmarks, you need a local Kafka cluster. You can run it yourself, but we recommend using the provided Docker Compose configuration. You can find it along with further instructions in the `scripts/kafka` directory. By default, the broker will listen on ports 9092, 9093, and 9094.

### Network emulation for end-to-end streaming benchmarks

To emulate specific network conditions in gRPC and Kafka benchmarks, you can use the [netem kernel module in Linux](https://man7.org/linux/man-pages/man8/tc-netem.8.html).

In our benchmarks we used the following configuration:

- Port 9092: no emulation
- Port 9093: 100 Mbit/s bandwidth, 10 ms latency
- Port 9094: 50 Mbit/s bandwidth, 15 ms latency

#### Enable netem (needs root privileges)

```shell
./scripts/set_netem.sh 10 100 9093 8421 2
./scripts/set_netem.sh 15 50 9094 8422 3
```

#### Check if netem is running

```shell
time nc -zw30 localhost 9092
time nc -zw30 localhost 9093
time nc -zw30 localhost 9094
```

The commands should show latencies of ~0ms, ~20ms, ~30ms (round-trip).

#### Disable netem

```shell
sudo tc qdisc del dev lo root
```

## License

The code in this repository was written by [Piotr Sowiński](https://ostrzyciel.eu) ([Ostrzyciel](https://github.com/Ostrzyciel)).

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
