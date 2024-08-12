- `./flat_ser_des.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/ 4096`
- `./flat_ser_des.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/ 1024`
- `./stream_ser_des.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/`
- `./size.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/`
- `./grpc_stream.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/ 8420`
- `./grpc_stream.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/ 8421`
- `./grpc_stream.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_100k/ 8422`
- `./grpc_latency.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_10k/ 8420`
- `./grpc_latency.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_10k/ 8421`
- `./grpc_latency.sh ~/work/graalvm21/bin/java ../target/scala-3.3.0/benchmarks-assembly-0.1.0-SNAPSHOT.jar ~/work/rb/jelly_10k/ 8422`

## Network emulation (end-to-end tests)

### Enable netem (needs root privileges)
```
./set_netem.sh 10 100 9093 8421 2
./set_netem.sh 15 50 9094 8422 3
```

### Check if netem is running
```
time nc -zw30 localhost 9092
time nc -zw30 localhost 9093
time nc -zw30 localhost 9094
```

The commands should show latencies of ~0ms, ~20ms, ~30ms (round-trip).

### Disable netem
```
sudo tc qdisc del dev lo root
```
