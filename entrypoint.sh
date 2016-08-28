#!/bin/bash

CMD="exec bin/storm jar ./topology.jar com.enow.storm.EnowTopology Enow "

echo "$CMD"
eval "$CMD"
