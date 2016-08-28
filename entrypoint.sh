#!/bin/bash

CMD="exec bin/storm jar ./topology.jar com.enow.storm.TestTopologyStaticHosts Enow "

echo "$CMD"
eval "$CMD"
