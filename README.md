ENOW-Server ![travis](https://travis-ci.org/ENOW-IJI/ENOW-server.svg?branch=master) [![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.storm/storm-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.storm"%20AND%20a:"storm-core")
=========================
```
███████╗███╗   ██╗ ██████╗ ██╗    ██╗  
██╔════╝████╗  ██║██╔═══██╗██║    ██║  
█████╗  ██╔██╗ ██║██║   ██║██║ █╗ ██║  
██╔══╝  ██║╚██╗██║██║   ██║██║███╗██║  
███████╗██║ ╚████║╚██████╔╝╚███╔███╔╝  
╚══════╝╚═╝  ╚═══╝ ╚═════╝  ╚══╝╚══╝   
ENOW-Server Version 0.0.1
Copyright © 2016 ENOW. All rights reserved.
```

How to use
---------------
- If you would like to implement ENOW-Server locally with IDE like eclipse, IntelliJ, run LocalSubmitter.

- Or you can use this command on the ENOW-Server directory.
  **1. Packaging Server**
  ```bash
  mvn clean package -P cluster
  ```
  **2. Run Server**
  ```bash
  $STORM_HOME/bin/storm jar ./target/enow-storm-1.0.jar \\
  com.enow.storm.main.main Trigger Action \\
  -c storm.local.hostname=\"nimbus\" \\
  -c nimbus.seeds=\"[\\\"192.168.99.100\\\"]\"
  ```
- Or you can simply use docker-compose to run ENOW-Server.

[https://github.com/ENOW-IJI/ENOW-docker](https://github.com/ENOW-IJI/ENOW-docker)

Todo List
---------
These are the new features you should expect in the coming months:
- [x] ~~Constructing development enviroment of Apache Storm~~
- [x] ~~Constructing development enviroment of Apache Kafka~~
- [x] ~~Constructing development enviroment of Apache Zookeeper~~
- [x] ~~Making Spout, Bolt, Topology in Apache Storm~~
- [x] ~~Kafka Integration for Apache Storm~~
- [x] ~~Making Connection between Ponte and Kafka~~
- [x] ~~Sending a message from Device(or Android) to Android(or Device) via ENOW System~~
- [x] ~~Not using LocalCluster, But using `StormSubmitter`~~
- [x] ~~Make Trigger & Status topologies~~
- [x] ~~Connect Apache Storm to Redis~~
- [x] ~~Create test Document on MongoDB~~
- [x] ~~Connect Apache Storm to MongoDB~~
- [x] ~~Connect Apache Storm to Redis for storing status of devices~~
- [x] ~~Connect Apache Storm to Console~~
- [x] ~~Connect Apache Storm to Devices~~
- [x] ~~Build on `StormSubmitter`~~
- [ ] Starting Apache Storm Server on ENOW-Console
- [ ] Send Info of Storm UI to ENOW-Console

[Apache Storm](http://storm.apache.org/)
------------
We use a free and open source distributed realtime computation system, [Apache Storm](http://storm.apache.org/). It's easy to reliably process unbounded streams of data, doing for realtime processing what Hadoop did for batch processing and simple, can be used with any programming language.

[Kafka Integration](http://kafka.apache.org/)
-----------------
[Apache Kafka](http://kafka.apache.org/) is publish-subscribe messaging rethought as a distributed commit log. A single Kafka broker can handle hundreds of megabytes of reads and writes per second from thousands of clients, so this is suitable for massive IoT platform.

Apache Storm basically support Kafka Integration by this self but we use `storm-kafka` instead of `storm-kafka-client`.

Topologies
----------
To do realtime computation on Apache Storm for ENOW, we create what are called "topologies". A topology is a graph of computation. Each node in a topology contains processing logic, and links between nodes indicate how data should be passed around between nodes.

TriggerTopology
---------------
TriggerTopology literally can activate the component of Road-Map so that IoT devices are connected each other. before ordering devices, TriggerTopology refine the information of event and link refined one to database for using.

<br>

### IndexingBolt :

__INPUT:__
- `eventKafka` from `Console` ⇨ `jsonObject(Event)`

- `orderKafka` from `user device` ⇨ `jsonObject(Order)`

- `proceedKafka` from `ActionTopology` ⇨ `jsonObject(Proceed)`


__`jsonObject(Event)` :__</br>
```JSON
{
    "roadMapId":"1",
}
```

__`jsonObject(orderKafka)` :__</br>
```JSON
{
    "corporationName":"enow",
    "serverId":"server0",
    "brokerId":"brokerId1",
    "deviceId":"deviceId1",
    "roadMapId":"1",
    "payload": {"humidity": "60", "brightness": "231"},
}
```

__`jsonObject(Proceed)` :__</br>
```JSON
{
    "topic":"enow/server0/brokerId1/deviceId1",
    "roadMapId":"1",
    "nodeId":"1",
    "incomingNode":["2", "4"],
    "outingNode":["11", "13"],
    "previousData":{"2" : "value1", "4" : "value2"},
    "payload": {"humidity": "60", "brightness": "231"},
    "lastNode":true,
    "order":false,
    "verified":false,
    "lambda":false
}
```

__PROCESSING:__

<!--
- `eventKafka`에서 `Console`로부터 받은 `jsonObject(Event)`를 받아온다.

- `orderKafka`에서 `user device`로부터 받은 `jsonObject(Order)`를 받아온다.

- `proceedKafka`에서 `ActionTopology`로부터 받은 `jsonObject(Proceed)`를 받아온다.

- `eventKafka`, `proceedKafka`와 `orderKafka`에서 받아온 `jsonObject`가 필요한 모든 `key`값을 갖고 있는지 확인한다.

- `orderKafka`에서 받아온 `jsonObject(Order)`는 사용자가 직접 보내준 값이므로 `brokerId`,`deviceId` 값이 `MongoDB`에 등록되어 있는지 확인한다.
-->

- Receive `jsonObject(Event)` from `console` at `eventKafka`

- Receive `jsonObject(Order)` from `user device` at `orderKafka`

- Receive `jsonObject(Proceed)` from `ActionTopology` at `proceedKafka`

- Verify `jsonObject` from `eventKafka`, `preceedKafka` and `orederKafka` whether it has all the neccessary `key` values  

- Verify whether values of `brokerId` and `deviceId` are registerd at `MongoDB` since `jsonObject(Order)` from `orderKafka` is obtained directly from the `user device`.



__OUTPUT:__
- `jsonObject` ⇨ `StagingBolt`

<br>

### StagingBolt :

__INPUT:__
- `IndexingBolt` ⇨  `jsonObject(Event)`

- `IndexingBolt` ⇨  `jsonObject(Order)`

- `IndexingBolt` ⇨  `jsonObject(Proceed)`

__PROCESSING:__

<!--
- `jsonObject(Event)`를 받은 경우 `MongoDB`에서 `jsonObject(Event)`의 `roadMapId`와 일치하는 `roadMapId`를 찾은 후 `initNode`들에 필요한 `key`들에 `value`값을 할당해준다.

- `jsonObject(Order)`를 받은 경우 `MongoDB`에서 `jsonObject(Order)`의 `roadMapId`와 일치하는 `roadMapId`를 찾은 후 `orderNode` 중 `jsonObject(Order)`의 `corporationName`,`serverId`,`brokerId`,`deviceId`와 일치하는 `orderNode`들에 필요한 `key`들에 `value`값을 할당해준다.

- `jsonObject(Proceed)`를 받은 경우 `MongoDB`에서 `jsonObject(proceed)`의 `roadMapId`와 일치하는 `roadMapId`를 찾은 후 `jsonObject(proceed)`의 `nodeId`와 일치하는 `nodeId`에 필요한 `key`들에 `value`값을 할당해준다.
-->

- If you have received `jsonObject(Event)`, find `roadMapId` consistent with the `roadMapId` of `jsonObject(Event)` at `MongoDB` and assign the values to the neccessary `keys` for `initNodes`

- If you have received `jsonObject(Order)`, find `roadMapId` consistent with the `roadMapId` of `jsonObject(Order)` at `MongoDB` and assign the values to the neccessary `keys` for `orderNodes` that corresponds to `corporationName`, `serverId`, `brokerId` and `deviceId` of `jsonObject(Order)` among `orderNodes`

- If you have received `jsonObject(Proceed)`, find `roadMapId` consistent with the `roadMapId` of `jsonObject(Proceed)` at `MongoDB` and assign the values to the neccessary `keys` for `nodeId` that corresponds to `nodeId` of `jsonObject(Proceed)`

__OUTPUT:__
- `jsonArray` ⇨ `CallingTriggerBolt`

<br>

### CallingTriggerBolt :

__INPUT:__
- `StagingBolt` ⇨ `jsonArray`

__PROCESSING:__

<!--
- `StagingBolt`에서 받은 `jsonArray`를 `jsonObject`로 바꿔 `Trigger topic`으로 보내준다.
-->

- Send the `Trigger topic` after changing `jsonArray` to `jsonObject` received from `StagingBolt`


__OUTPUT:__
- `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `topic : Trigger`

__`jsonObject.toJSONString` :__</br>
```JSON
{
    "topic":"enow\/serverId1\/brokerId1\/deviceId1",
    "roadMapId":"1",
    "mapId":"1",
    "incomingNode":null,
    "outingNode":["2"],
    "previousData":null,
    "payload":null,
    "lastNode":false,
    "order":false,
    "verified":true,
    "lambda":false,
}
```

<br><br>

ActionTopology
--------------
ActionTopology schedules the enow server to run a Road-Map scheme and handles the executable source code from console pairing the result to devices and event came from TriggerTopology.

### StatusBolt :

__INPUT:__
- `statusKafka` ⇨ `jsonObject(Status)`

__`jsonObject(Status)` :__</br>
```JSON
{
    "topic":"enow\/serverId1\/brokerId1\/deviceId1",
    "payload": {"humidity": "60", "brightness": "231"}
}
```

__PROCESSING:__

<!--
- `jsonObject(Status)`들을 `topic`별로 `Redis`에 저장한다.
-->

- Save `jsonObject(Status)` by `topic` in `redis`


__OUTPUT:__
- none.

<br>

### SchedulingBolt :

__INPUT:__
- `triggerKafka` ⇨ `jsonObject(Trigger)`

__PROCESSING:__

<!--
- Status 토픽에서 들어온 정보를 현재 처리중인 노드와 동기화한다. (만약 해당 노드가 orderNode나 lambdaNode 라면 이 과정을 무시한다.)
- 이전 노드에서 값을 받아오는 중간 단계 노드라면 Redis에 저장된 payload 값들을 꺼내 previousData에 저장한다.
- 이전 노드의 처리가 다 끝나지 않았다면 verified 값을 false 로 전환하고, 다음 stream 을 기다린다.
- 만약 n:n 처리를 위해 서버에 여러번 방문하는 노드라면 Redis에 해당 노드의 정보가 저장되어있는지 확인 후 redundancy 값으로 인식하여 verifiec 값을 false 로 변경한다.
-->

- If `order` value is false or `lambda` value is false and current node has multiple `incomingNode`, save `payload` information for each node at `jsonObject` based on the `topic` of current `jsonObject` from `redis`

- If `incomingNode` is not null and it is first visit for `nodeId`, bring the saved valued at each `incomingNode` from `redis` and save in `previousData` of `jsonObject`

- If is is not first visit, change the `verified` value to false

__OUTPUT:__
- `jsonObject` ⇨ `ExecutingBolt`

<br>

### ExecutingBolt :

__INPUT:__
- `SchedulingBolt` ⇨ `jsonObject`

__PROCESSING:__

- Receive `source` and `parameter` from `MongoDB`

- Extract `payload` from `jsonObject`

- Put the `payload` and the `parameter` from source and execute.
The condition is as follow
  * if the program is `lambda`, the `payload` has empty STRING
  * if the program is `device`, the `payload` has json STRING with value passed from the device

__OUTPUT:__
- `jsonObject` ⇨ `ProvisioningBolt`

<br>

### ProvisioningBolt :

__INPUT:__
- `ExecutingBolt` ⇨ `jsonObject`

__PROCESSING:__

<!--
- `ExecutingBolt`에서 실행 후 `result`를 포함한 `jsonObject`를 `Redis`에 저장하고, `refer`값을 갱신한다.
-->

- Save `jsonObject` which includes `result`( the result of running `ExecutingBolt`) at `redis` and update the `refer` value.

__OUTPUT:__
- `jsonObject` ⇨ `CallingFeedBolt`

<br>

### CallingFeedBolt :

__INPUT:__
- `ProvisioningBolt` ⇨ `jsonObject`

__PROCESSING:__

<!--
- `ProvisioningBolt`에서 받은 `jsonObject`의 `nodeId`를 `jsonObject`의 `outingNode`들의 `nodeId` 별로 바꿔 `Feed topic`,`Proceed topic`으로 보내준다.

- `rambda`값이 `true`일 경우에는 `Feed topic`으로 보내지 않는다.
-->

- After changing `nodeId` of `jsonObject` from `ProvisioningBolt` to `outingNodes` send to `Feed topic`, `Preceed topic`

- If `lambda` value is true, do not send to `Feed topic`

__OUTPUT:__
- `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `topic : Feed`

- `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `topic : Proceed`

<br><br>

Payload
=======

__JsonObject :__</br>
```JSON
{
    "topic":"enow/serverId1/brokerId1/deviceId1",
    "roadMapId":"1",
    "mapId":"1",
    "incomingNode":["2", "4"],
    "outingNode":["11", "13"],
    "previousData":{"2" : "value1", "4" : "value2"},
    "payload": {"humidity": "60", "brightness": "231"},
    "lastNode":false,
    "order":false,
    "verified":false
}

```

<!--
topic : corporationName/serverId/brokerId(ip of user's mqtt)/deviceId 으로 구성.
roadMapId : road map id
nodeId : 현재 node id
incomingNode : 현재 nodeId로 들어오는 node들
outingNode : 현재 nodeId에서 나가는 node들
previousData : 이전 node들의 실행 결과
payload : 현제 node의 센서값 혹은 사용자로부터 들어온 값
lastNode : 마지막 노드라면 true,아니라면 false
orderNode : 사용자가 직접 입력하는 형태라면 true, 아니라면 false
verified : incomingNode들이 모두 들어왔다면 true, 아니라면 false
-->

- topic : it is consists of corporationName/serverId/brokerId(ip of user's mqtt)/deviceId

- roadMapId : road map id

- nodeId : current node id

- incomingNode : node coming from current nodel

- outingNode : node that goes out from current node

- previousData : execution result from past node

- payload : sensor value of current node or value reiceived from the user

- lastNode : true for last node, false for contrary

- orderNode : true if it is the user directly inserting type, false for contrary

- verified : true if all incomingNode are in, false for contrary

__Status :__ </br>
```JSON
{
  "topic":"enow/serverId1/brokerId1/deviceId1",
  "payload": {"humidity": "60", "brightness": "231"}
}
```

__ExecutingBolt :__ </br>
result: <br>
```JSON
{
    "callback": "{}",
    "event": "{\"identification\": \"modified\"}",
    "context": "{\"previousData\": {\"key\": \"value\"}, \"topicName\": \"\", \"deviceID\": \"\", \"invoked_ERN\": \"\", \"function_name\": \"\", \"parameter\": \"\\n-ls -t\\n\", \"function_version\": \"\", \"memory_limit_in_mb\": 64}"
}
```


```JSON
{
 "PARAMETER" : "STRING",
 "PAYLOAD" : "JSON STRING",
 "SOURCE" : "STRING",
 "previousData" : [
   {"RESULT 1" : "JSON STRING"},
   {"RESULT 2" : "JSON STRING"},
   {"RESULT N" : "JSON STRING"}
 ]
}
```

> `_"previousData"_` : `ExecutingBolt` has result of previous execution. numbering of `RESULT N` follows _stack_ rule and if the number of `RESULT N` is higher the result value is more lastest.<br><br>
`_"PARAMETER"_` : delivers necessary parameter to run.<br><br>
__i.e)__ ⇨ _"-l --profile"_<br><br>
`_"PAYLOAD"_` : delivers necessary input value to run.<br><br>
  __i.e)__ ⇨
  _"{
    'led on' : 1,
    'match object' : 'cat'}
  }"_<br><br>
`_"SOURCE"_` : delivers necessary source(`PYTHON`) to run.<br><br>
__i.e)__ ⇨ _"def eventHandler(event, context, callback):\n\tevent[\"identification\"] = \"modified\"\n\tprint(\"succeed\")"_


necessary source for run is as follow

  >`def eventHandler(event, context, callback):`

References
==========

Test project for enow-storm based on information provided in and referenced by:

Apache Storm & Kafka:
- [https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java](https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java)
- [https://github.com/nathanmarz/storm/wiki/Trident-tutorial](https://github.com/nathanmarz/storm/wiki/Trident-tutorial)
- [https://github.com/nathanmarz/storm/wiki/Trident-state](https://github.com/nathanmarz/storm/wiki/Trident-state)
- [https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example)
- [https://github.com/wurstmeister/storm-kafka-0.8-plus-test](https://github.com/wurstmeister/storm-kafka-0.8-plus-test)
