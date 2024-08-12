# jvm-benchmarks

Benchmarks for the Jelly JVM implementation

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
