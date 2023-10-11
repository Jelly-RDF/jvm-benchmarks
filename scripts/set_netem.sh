#!/bin/bash

set -ux

DELAY_MS=$1
RATE_MBIT=$2
PORT_KAFKA=$3
PORT_GRPC=$4
FLOW=$5

BUF_PKTS=33
BDP_BYTES=$(echo "($DELAY_MS/1000.0)*($RATE_MBIT*1000000.0/8.0)" | bc -q -l)
BDP_PKTS=$(echo "$BDP_BYTES/1500" | bc -q)
LIMIT_PKTS=$(echo "$BDP_PKTS+$BUF_PKTS" | bc -q)

# Set limit
sudo tc qdisc add dev lo root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
sudo tc qdisc add dev lo parent 1:$FLOW handle "$FLOW"0: netem delay ${DELAY_MS}ms rate ${RATE_MBIT}Mbit limit ${LIMIT_PKTS}

# Set filter for Kafka
sudo tc filter add dev lo parent 1:0 protocol ip u32 match ip sport "$PORT_KAFKA" 0xffff flowid 1:$FLOW
sudo tc filter add dev lo parent 1:0 protocol ip u32 match ip dport "$PORT_KAFKA" 0xffff flowid 1:$FLOW

# Set filter for gRPC
sudo tc filter add dev lo parent 1:0 protocol ip u32 match ip sport "$PORT_GRPC" 0xffff flowid 1:$FLOW
sudo tc filter add dev lo parent 1:0 protocol ip u32 match ip dport "$PORT_GRPC" 0xffff flowid 1:$FLOW
