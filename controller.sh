#!/bin/bash

DIR_KAFKA=~/kafka
DIR_JAVA=~/Distributed-system-project/kafka/target/classes/kafka/airportManager

start_java () {
    /usr/lib/jvm/java-11-openjdk-amd64/bin/java -Dfile.encoding=UTF-8 -classpath /home/alumno/Distributed-system-project/kafka/target/classes:/home/alumno/.m2/repository/org/apache/kafka/kafka-clients/2.6.0/kafka-clients-2.6.0.jar:/home/alumno/.m2/repository/com/github/luben/zstd-jni/1.4.4-7/zstd-jni-1.4.4-7.jar:/home/alumno/.m2/repository/org/lz4/lz4-java/1.7.1/lz4-java-1.7.1.jar:/home/alumno/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.3/snappy-java-1.1.7.3.jar:/home/alumno/.m2/repository/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar:/home/alumno/.m2/repository/org/slf4j/slf4j-simple/1.7.30/slf4j-simple-1.7.30.jar:/home/alumno/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.11.2/jackson-databind-2.11.2.jar:/home/alumno/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.11.2/jackson-annotations-2.11.2.jar:/home/alumno/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.11.2/jackson-core-2.11.2.jar kafka.airportManager.$1
}

producer_start () {
    start_java AirportDepartureProducer &
    start_java AirportArrivalProducer &
    start_java ParkingLotsProducer &
}

producer_stop () {
    PRODUCER_PIDS=$(pgrep -f 'bin/java')
    test -n "$PRODUCER_PIDS" &&
        kill $PRODUCER_PIDS
}

if [ "$1" = "start" ]; then
    echo "Starting servers..."

    # node-red
    node-red &

    # kafka
    cd $DIR_KAFKA
    bin/zookeeper-server-start.sh config/zookeeper.properties &
    bin/kafka-server-start.sh config/server.properties --override delete.topic.enable=true &
    sleep 15
    TOPICS=$(bin/kafka-topics.sh --list --zookeeper localhost:2181)

    ! echo "$TOPICS" | grep -q 'AirportDep' &&
        bin/kafka-topics.sh --create --topic 'AirportDep' --partitions 4 --bootstrap-server localhost:9092

    ! echo "$TOPICS" | grep -q 'AirportArr' &&
        bin/kafka-topics.sh --create --topic 'AirportArr' --partitions 4 --bootstrap-server localhost:9092

    ! echo "$TOPICS" | grep -q 'ParkingLots' &&
        bin/kafka-topics.sh --create --topic 'ParkingLots' --partitions 3 --bootstrap-server localhost:9092

    sleep 5
    echo -e "\n\nDone"
elif [ "$1" = "delete" ]; then
    cd $DIR_KAFKA
    bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic AirportDep
    bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic AirportArr
    bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ParkingLots
elif [ "$1" = "info" ]; then
    cd $DIR_KAFKA
    bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic AirportDep
    bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic AirportArr
    bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic ParkingLots
elif [ "$1" = "fix" ]; then
    cd $DIR_KAFKA
    bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic AirportDep --partitions 4
    bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic AirportArr --partitions 4
    bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic ParkingLots --partitions 3
elif [ "$1" = "pstart" ]; then
    echo "Starting producers..."
    producer_start
elif [ "$1" = "pstop" ]; then
    echo "Stopping producers..."
    producer_stop
elif [ "$1" = "stop" ]; then
    echo "Stopping everything..."

    # java
    producer_stop

    # kafka
    cd $DIR_KAFKA
    bin/kafka-server-stop.sh
    bin/zookeeper-server-stop.sh
    rm -Rf /tmp/kafka-logs /tmp/zookeeper

    # node-red
    NODE_PID=$(pgrep node-red)
    test -n "$NODE_PID" &&
        kill "$NODE_PID"

    echo -e "\n\nDone"
fi
