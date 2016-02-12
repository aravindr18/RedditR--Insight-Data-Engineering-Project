#!/bin/bash
# start hadoop
$HADOOP_HOME/sbin/start-all.sh
sleep 10

# start spark
$SPARK_HOME/sbin/start-master.sh
sleep 20
$SPARK_HOME/sbin/start-slaves.sh

# start zookeeper
sudo /usr/local/zookeeper/bin/zkServer.sh start
# check if it's running:
echo srvr | nc localhost 2181 | grep Mode

# start kafka
sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
sleep 5
$KAFKA_MANAGER_HOME/bin/kafka-manager -Dhttp.port=9001 &

# start storm
sudo $STORM_HOME/bin/storm nimbus &
sleep 20
sudo $STORM_HOME/bin/storm ui &


