#!/bin/bash
docker run --link storm_kafka_1:kafka --rm -v /var/run/docker.sock:/var/run/docker.sock -e HOST_IP=$1 -e ZK=$2 -i -t enow/kafka:latest /bin/bash
