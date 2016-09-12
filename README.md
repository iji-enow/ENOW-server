TEST-storm ![travis](https://travis-ci.org/ENOW-IJI/storm.svg?branch=master) [![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.storm/storm-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.storm"%20AND%20a:"storm-core")
=========================



Todo List
---------
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

#### Peers

- outingPeer
- incomingPeer
- Peers

####

Topologies
==========
hihihi
#### TriggerTopology
##### IndexingBolt :
1. `eventKafka` 토픽에서 들어온 `JSON`꼴의 `String`을 `jsonObject`로 바꿔준다.
- `ack = false`일 경우 `HashMap`에 해당 토픽을 저장해놓는다.
- `ack = true`일 경우 `HashMap`에 해당하는 토픽이 있는지 확인 후 있다면 넘겨주고 없다면 무시한다.

##### StagingBolt :
1. 현재 노드에 해당하는 `serverId`, `brokerId`, `deviceId`, `phaseRoadMapId`의 값들이 유효한 값인지 검증한다.
- 현재 노드에 해당하는 `mapId`와 `phaseId`를 추출하여 `jsonObject`에 추가시켜준다.
- `ack = true`일 경우 현재 노드의 `peerOut`, `peerIn` 값들을 확인하여 `jsonObject`에 저장한다.
- `ack = true`일 경우 현재 노드의 `phaseLastNode`과 다음 `phase`의 `phaseInitNode` 값들을 확인하여 `jsonObject`에 저장한다.
- `JSON`과 위에서 거친 검증 값에 따라 `CallingTriggerBolt`로 넘겨 준다.

##### CallingTriggerBolt :
1. `StagingBolt`에서 넘겨준 `jsonObject`를 받아 `triggerKafka`로 넘겨준다.

#### ActionTopology

##### SchedulingBolt :
INPUT:
- `triggerKafka` ⇨ `jsonObject`
- `statusKafka` ⇨ `status`

PROCESSING:
- 현재 노드가 다수의 `incomingPeer`들을 가질 때, 이를 `Redis`에 저장한다. <br>(추후, 해당 노드에 대해선 CallingFeed가 일어나지 않는다.)
- When node needs multiple `previousData`, wait for all of `incomingPeers`

OUTPUT:
- `jsonObject` ⇨ `ExecutingBolt`

##### ExecutingBolt :
INPUT:
- `SchedulingBolt` ⇨ `jsonObject`

PROCESSING:
- `MongoDB`에서 Source Code 와 Parameter를 받아온다.

OUTPUT:
- `jsonObject` ⇨ `ProvisioningBolt`

##### ProvisioningBolt :
INPUT:
- `ExecutingBolt` ⇨ `jsonObject`

PROCESSING:
- 현재 노드가 다수의 `outingPeer`들을 가질 때, 이를 `Redis`에 저장한다.

OUTPUT:
- `jsonObject` ⇨ `CallingFeedBolt`

###### CallingFeedBolt :
INPUT:
- `ProvisioningBolt` ⇨ `jsonObject`

PROCESSING:
- 

OUTPUT:
- `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `Topic : Feed`

Payload
=======

__JsonObject :__</br>
```JSON

{
    "corporationName":"enow",
    "serverId":"serverId1",
    "brokerId":"brokerId1",
    "deviceId":"deviceId1",
    "roadMapId":"1",
    "mapId":1,
    "procced":false,
    "waitingPeer":["2", "4"],
    "outingPeer":["11", "13"],
    "previousData":[{},{},{}],
    "payload":[]
}
```
__Status :__ </br>
```JSON
{
  "topic":"enow/serverId1/brokerId1/deviceId1"
}
```
__ExecutingBolt :__ </br>
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
__i.e)__ -> _"-l --profile"_<br><br>
`_"PAYLOAD"_` : 실행에 필요한 입력값을 전달해 준다.<br><br>
  __i.e)__ ->
  _"{
    'led on' : 1,
    'match object' : 'cat'}
  }"_<br><br>
`_"SOURCE"_` : 실행에 필요한 소스(`PYTHON`)를 전달해 준다.<br><br>
__i.e)__ -> _"def eventHandler(event, context, callback):\n\tevent[\"identification\"] = \"modified\"\n\tprint(\"succeed\")"_

  실행에 필요한 소스의 기본형은 다음과 같다.

  >`def eventHandler(event, context, callback):`

References
==========

Test project for enow-storm based on information provided in and referenced by:

- [https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java](https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java)
- [https://github.com/nathanmarz/storm/wiki/Trident-tutorial](https://github.com/nathanmarz/storm/wiki/Trident-tutorial)
- [https://github.com/nathanmarz/storm/wiki/Trident-state](https://github.com/nathanmarz/storm/wiki/Trident-state)
- [https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example)
- [https://github.com/wurstmeister/storm-kafka-0.8-plus-test](https://github.com/wurstmeister/storm-kafka-0.8-plus-test)
