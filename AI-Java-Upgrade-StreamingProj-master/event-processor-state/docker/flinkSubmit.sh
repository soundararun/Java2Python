#!/bin/bash
cd /opt/zif/flink-1.12.1/bin

./flink run /opt/zif/event-processor-state-1.0.jar --kafka-host $1 --kafka-port $2 --data-topic $3 --parallelism-factor $4 --partition-key $5