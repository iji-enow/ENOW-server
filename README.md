TEST-storm ![travis](https://travis-ci.org/ENOW-IJI/storm.svg?branch=master) [![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.storm/storm-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.storm"%20AND%20a:"storm-core")
=========================



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
- [ ] Make Event topology
- [ ] Make Trigger & Status topology
- [x] ~~Syncronizing data stream with `ConcurrentHashMap`~~
- [x] ~~Create test Document on MongoDB~~
- [x] ~~Connect Apache Storm to MongoDB~~
- [x] ~~Connect Apache Storm to Redis for storing status of server~~
- [ ] Connect Apache Storm to Console
- [ ] Connect Apache Storm to Devices
- [ ] Build on `StormSubmitter`

Elements
========

### Nodes
- outingNode
- incomingNode
- Nodes

Topologies
==========
To do realtime computation on Apache Storm for ENOW, we create what are called "topologies". A topology is a graph of computation. Each node in a topology contains processing logic, and links between nodes indicate how data should be passed around between nodes.

TriggerTopology
---------------
TriggerTopology literally can activate the component of Road-Map so that IoT devices are connected each other. before ordering devices, TriggerTopology refine the information of event and link refined one to database for using.

### IndexingBolt :
INPUT:
 > `eventKafka` ⇨ `jsonObject(Event)`
 <br>`proceedKafka` ⇨ `jsonObject(Proceed)`

PROCESSING:
 > `eventKafka`에서 `Console`에서 `Init node`의 정보를 받아온다.
 <br><br>`proceedKafka`에서는 `ActionTopology`에서 `running node`의 정보를 받아온다.
 <br><br>`eventKafka`와 `proceedKafka`에서 받아온 `jsonObject`가 필요한 모든 <br>`key value`를 갖고 있는지 확인한다.
 <br><br>`eventKafka`와 `proceedKafka`에서 받아온 `jsonObject`의 `serverId`,`brokerId`,`deviceId` 값이 `MongoDB`에 등록되어 있는지 확인한다.

 OUTPUT:
 > `jsonObject` ⇨ `StagingBolt`


### StagingBolt :
INPUT:
> `IndexingBolt` ⇨ `jsonObject`

PROCESSING:
> `"init" = true`라면 받은 `jsonObject` 그대로 `CallingTriggerBolt`로 넘겨준다.
<br><br>`"init" = flase`라면 받은 `jsonObject`의 `MapID`값에 해당하는 `deviceId`, `waitingNode`, `outingNode`의 값을 `MongoDB`에서 받아와 갱신시켜준다.

OUTPUT:
> `jsonObject` ⇨ `CallingTriggerBolt`

### CallingTriggerBolt :
INPUT:
> `StagingBolt` ⇨ `jsonObject`

PROCESSING:
> `StagingBolt`에서 받은 `jsonObject`를 `Trigger topic`으로 보내준다.

OUTPUT:
> `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `Topic : Trigger`

ActionTopology
--------------
ActionTopology schedules the enow server to run a Road-Map scheme and handles the executed source code from console pairing the result to devices and event came from TriggerTopology.
### SchedulingBolt :
INPUT:
> `triggerKafka` ⇨ `jsonObject(Trigger)`
<br>`statusKafka` ⇨ `jsonObject(Status)`

PROCESSING:
> 현재 노드가 다수의 `incomingNode`들을 가질 때, 각각 노드들의 `payload` 정보를 `Redis`에서 읽어들여 `jsonObject`에 저장한다. <br>
각각의 Trigger signal들을 Redis에 저장된 device 데이터들과 매칭시켜 jsonObject에 저장한다.<br>
변경된 `jsonObject`를 `ExecutingBolt`로 `emit` 한다.
<br>When node needs multiple `previousData`, wait for all of `incomingNodes`

OUTPUT:
> `jsonObject` ⇨ `ExecutingBolt`

### ExecutingBolt :
INPUT:
> `SchedulingBolt` ⇨ `jsonObject`

PROCESSING:
> `MongoDB`에서 Source Code 와 Parameter를 받아온다.

OUTPUT:
> `jsonObject` ⇨ `ProvisioningBolt`

### ProvisioningBolt :
INPUT:
> `ExecutingBolt` ⇨ `jsonObject`

PROCESSING:
> 갱신된 `result`를 해당 노드에 매칭된 `Redis`에 갱신한다.

OUTPUT:
> `jsonObject` ⇨ `CallingFeedBolt`

### CallingFeedBolt :
INPUT:
> `ProvisioningBolt` ⇨ `jsonObject`

PROCESSING:
> `ProvisioningBolt`를 참고하여 `KafkaProducer`를 호출한다.<br>
  `outingNode`의 `MapID`별로 `jsonObject`를 갱신시킨 후 `KafkaProducer`를 호출한다.<br>
  `order`값을 통해

OUTPUT:
> `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `Topic : Feed`

StatusTopology
--------------

### StatusBolt :
INPUT:
> `statusKafka` ⇨ `jsonObject(Status)`

PROCESSING:
> `Topic`별로 디바이스 정보를 `Redis`에 저장한다.

OUTPUT:
> `jsonObject` ⇨ `ExecutingBolt`

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
    "payload": {},
    "lastNode":false,
    "order":false,
    "varified":false
}
```
order 0: 반복되는 시퀸스
order 1: 트리거형 시퀸스의 중간 노드들
order 2: 트리거형 시퀸스의 마지막 노드
source, parameter 들은 따로 MongoDB를 통해 Console로부터 값을 받아온다.
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
