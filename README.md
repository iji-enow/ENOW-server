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
- [x] Make Trigger & Status topology
- [x] ~~Connect Apache Storm to Redis~~
- [x] ~~Create test Document on MongoDB~~
- [x] ~~Connect Apache Storm to MongoDB~~
- [x] ~~Connect Apache Storm to Redis for storing status of server~~
- [ ] Connect Apache Storm to Console
- [ ] Connect Apache Storm to Devices
- [ ] Build on `StormSubmitter`

Apache Storm
------------
We use a free and open source distributed realtime computation system, Apache Storm. It's easy to reliably process unbounded streams of data, doing for realtime processing what Hadoop did for batch processing and simple, can be used with any programming language.

Topologies
----------
To do realtime computation on Apache Storm for ENOW, we create what are called "topologies". A topology is a graph of computation. Each node in a topology contains processing logic, and links between nodes indicate how data should be passed around between nodes.

TriggerTopology
---------------
TriggerTopology literally can activate the component of Road-Map so that IoT devices are connected each other. before ordering devices, TriggerTopology refine the information of event and link refined one to database for using.

### IndexingBolt :

__INPUT:__
- `eventKafka` ⇨ `jsonObject(Event)` from `Console`
- `orderKafka` ⇨ `jsonObject(Order)` from `user device`
- `proceedKafka` ⇨ `jsonObject(Proceed)`from `ActionTopology`


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
    "serverId":"serverId1",
    "brokerId":"brokerId1",
    "deviceId":"deviceId1",
    "roadMapId":"1",
    "payload": {"humidity": "60", "brightness": "231"},
}
```

__`jsonObject(Proceed)` :__</br>
```JSON
{
    "topic":"enow/serverId1/brokerId1/deviceId1",
    "roadMapId":"1",
    "mapId":"1",
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
 From `eventKafka` get `jsonObject(Event)`
 From `orderKafka` get `jsonObject(Order)`
 From `proceedKafka` get `jsonObject(Proceed)`
 Check whether `jsonObject` has all necessary `key` values
 Since `jsonObject(Order)` is from `user device` directly confirm whether `serverId`,`brokerId`and `deviceId` values are all registered in `MongoDB`
 -->
- `eventKafka`에서 `Console`로부터 받은 `jsonObject(Event)`를 받아온다.

- `orderKafka`에서 `user device`로부터 받은 `jsonObject(Order)`의 정보를 받아온다.

- `proceedKafka`에서 `ActionTopology`로부터 받은 `jsonObject(Proceed)`를 받아온다.

- `eventKafka`, `proceedKafka`와 `orderKafka`에서 받아온 `jsonObject`가 필요한 모든 `key`값을 갖고 있는지 확인한다.

- `orderKafka`에서 받아온 `jsonObject`는 사용자가 직접 보내준 값이므로 `corporationName`,`serverId`,`brokerId`,`deviceId` 값이 `MongoDB`에 등록되어 있는지 확인한다.


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
If `jsonObject(Event)` is received, find `roadMapId` which is same as `roadMapId` in `jsonObject(Event)` then start `initNodes`
-->
- `jsonObject(Event)`를 받은 경우 `MongoDB`에서 `jsonObject(Event)`의 `roadMapId`와 일치하는 `roadMapId`를 찾아 `initNode`들을 실행한다.

- `jsonObject(Order)`를 받은 경우 `MongoDB`에서 `jsonObject(Order)`의 `roadMapId`와 일치하는 `roadMapId`를 찾아 `orderNode` 중 `jsonObject(Order)`의 `corporationName`,`serverId`,`brokerId`,`deviceId`와 일치하는 `mapId`를 실행한다.

- `proceedKafka`에서 `jsonObject(Proceed)`를 받은 경우 `MongoDB`에서 `jsonObject(proceed)`의 `roadMapId`와 일치하는 `roadMapId`를 찾은 후  `jsonObject(proceed)`의 `mapId`와 일치하는 `mapId`의 `incomingNode`와 `outingNode`를 `jsonObject(Proceed)`에 할당해준다.

__OUTPUT:__
- `jsonArray` ⇨ `CallingTriggerBolt`

<br>

### CallingTriggerBolt :

__INPUT:__
- `StagingBolt` ⇨ `jsonArray`

__PROCESSING:__
- `StagingBolt`에서 받은 `jsonArray`를 `jsonObject`로 바꿔 `Trigger topic`으로 보내준다.

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
- `JsonObject(Status)`들을 `topic`별로 `Redis`에 저장한다.
<!--
Save Each `JsonObjects` came from `StatusKafka` to `Redis`.
-->

__OUTPUT:__
- 없다.

<br>

### SchedulingBolt :

__INPUT:__
- `triggerKafka` ⇨ `jsonObject(Trigger)`

__PROCESSING:__
- `order`값이 `false` 또는 `rambda`값이 `false`일 때, 현재 노드가 다수의 `incomingNode`들을 가지면, 각각 노드들의 `payload` 정보를 현재 `jsonObject`가 지닌 `topic`에 근거하여 `Redis`에서 읽어들여 `jsonObject`에 저장한다.

- `incomingNodes`가 `null`이 아니고, 처음 방문한 `mapId`라면 `Redis`에서 각각 `incomingNodes`에 저장된 값들을 꺼내 `jsonObject`안의 `previousData`에 저장한다.

- 만약 이미 방문한적이 있는 `mapId`라면 `verified`값을 `false`로 전환한다.

- `verified`값이 `true`면 각각의 `Trigger` 토픽에서 쏟아져나오는 `jsonObject`값들을 `Redis`에 저장한다.

<!-- When node needs multiple `previousData`, wait for all of `incomingNodes`
-->

__OUTPUT:__
- `jsonObject` ⇨ `ExecutingBolt`

<br>

### ExecutingBolt :

__INPUT:__
- `SchedulingBolt` ⇨ `jsonObject`

__PROCESSING:__
- `MongoDB`에서 source code 와 parameter를 받아온다.

- `jsonObject`에서 `payload`를 받아온다.

- `source code`에 `parameter`와 `payload`값을 넣어 실행한다. 단, `lambda`가 `true`일 경우에는 `payload` 값은 `""`이다.

__OUTPUT:__
- `jsonObject` ⇨ `ProvisioningBolt`

<br>

### ProvisioningBolt :

__INPUT:__
- `ExecutingBolt` ⇨ `jsonObject`

__PROCESSING:__
- `ExecutingBolt`에서 실행한 `jsonObject`의 `topic`과 일치하는 `topic`을 `Redis`에서 찾아 갱신하여준다.

__OUTPUT:__
- `jsonObject` ⇨ `CallingFeedBolt`

<br>

### CallingFeedBolt :

__INPUT:__
- `ProvisioningBolt` ⇨ `jsonObject`

__PROCESSING:__
- `ProvisioningBolt`에서 받은 `jsonObject`의 `mapId`를 `jsonObject`의 `outingNode`들의 `mapId` 별로 바꿔 `Feed topic`,`Proceed topic`으로 보내준다.

- `rambda`값이 `true`일 경우에는 `Feed topic`로 보내지 않는다.

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

topic : 회사명/서버명/브로커명(사용자의 mqtt의 ip)/디바이스명 으로 구성.
roadMapId : 로드맵 아이디
mapId : roadMapId안에 존재하는 노드들의 아이디
incomingNode : 현재 mapId로 들어오는 node들
outingNode : 현재 mapId에서 나가는 node들
previousData : 이전 node들의 실행 결과
payload : 현제 node의 센서값 혹은 사용자로부터 들어온 값
lastNode : 마지막 노드라면 true,아니라면 false
order : 사용자가 직접 입력하는 형태라면 true, 아니라면 false
verified : incomingNode들이 모두

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

> `_"previousData"_` : 이전에 실행했던 `ExecutingBolt` 에서의 결과값을 담고 있다. `RESULT N` 에서의 넘버링은 _스택_ 의 규칙을 따르며 넘버링이 클 수록 최근의 결과값이다.<br><br>
`_"PARAMETER"_` : 실행에 필요한 파라미터를 세팅하여 넘겨준다.<br><br>
__i.e)__ ⇨ _"-l --profile"_<br><br>
`_"PAYLOAD"_` : 실행에 필요한 입력값을 전달해 준다.<br><br>
  __i.e)__ ⇨
  _"{
    'led on' : 1,
    'match object' : 'cat'}
  }"_<br><br>
`_"SOURCE"_` : 실행에 필요한 소스(`PYTHON`)를 전달해 준다.<br><br>
__i.e)__ ⇨ _"def eventHandler(event, context, callback):\n\tevent[\"identification\"] = \"modified\"\n\tprint(\"succeed\")"_

  실행에 필요한 소스의 기본형은 다음과 같다.

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

Redis:
- [https://github.com/CaptBoscho/SOC-Redis-Plugin/blob/master/src/main/java/daos/UserDAO.java](https://github.com/CaptBoscho/SOC-Redis-Plugin/blob/master/src/main/java/daos/UserDAO.java)

MongoDB:
