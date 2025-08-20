#!/bin/bash

echo "Unpack Flink"

cd /opt/zif

tar -zxvf flink-1.12.1-bin-scala_2.11.tgz

#rm -f ./opt/zif/flink-1.12.1-bin-scala_2.11.tgz

#sed -i "s/taskmanager.memory.process.size: 1568m/taskmanager.memory.process.size: $FLINK_MANAGED_MEMORY/" /opt/zif/flink-1.12.1/conf/flink-conf.yaml
sed -i "s/jobmanager.memory.process.size: 1600m/jobmanager.memory.process.size: $FLINK_JOB_MANAGER_SIZE/" /opt/zif/flink-1.12.1/conf/flink-conf.yaml
sed -i "s/taskmanager.memory.process.size: 1728m/taskmanager.memory.process.size: $FLINK_TASK_MANAGER_SIZE/" /opt/zif/flink-1.12.1/conf/flink-conf.yaml
sed -i "s/# state.backend: filesystem/state.backend: rocksdb/" /opt/zif/flink-1.12.1/conf/flink-conf.yaml
sed -i "s/# state.backend.incremental: false/state.backend.incremental: true/" /opt/zif/flink-1.12.1/conf/flink-conf.yaml
sed -i "s_# state.checkpoints.dir: hdfs://namenode-host:port/flink-checkpoints_state.checkpoints.dir: file:///opt/flink/data_" /opt/zif/flink-1.12.1/conf/flink-conf.yaml
#sed -i "s_# state.savepoints.dir: hdfs://namenode-host:port/flink-savepoints_state.savepoints.dir: file:///opt/flink/data_" /opt/zif/flink-1.12.1/conf/flink-conf.yaml

cd /opt/zif/flink-1.12.1/bin
./start-cluster.sh

echo "Cluster is started"

sleep 80

echo "sleeping"

cd /opt/zif

echo "inside zif"

./flinkSubmit.sh $KAFKA_HOST $KAFKA_PORT zif_cpuMetrics,zif_memoryMetrics,zif_diskMetrics,zif_infraMetrics,syslogs,snmp_traps 1 group_name,resource_type

set -x
while $1
do
 echo "Press [CTRL+C] to stop.."
 sleep 50000
    echo "Flink docker is running"
done
