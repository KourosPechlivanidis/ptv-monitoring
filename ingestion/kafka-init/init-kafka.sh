#!/bin/bash
echo "Creating Kafka topics..."
kafka-topics \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic vehicle_positions \
    --replication-factor 1 \
    --partitions 2 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000


kafka-topics \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic trip_updates \
    --replication-factor 1 \
    --partitions 2 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000


echo "Successfully created topics:"
kafka-topics --bootstrap-server kafka:9092 --list
