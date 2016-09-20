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
- [x] Make Trigger & Status topology
- [x] ~~Connect Apache Storm to Redis~~
- [x] ~~Create test Document on MongoDB~~
- [x] ~~Connect Apache Storm to MongoDB~~
- [x] ~~Connect Apache Storm to Redis for storing status of server~~
- [ ] Connect Apache Storm to Console
- [ ] Connect Apache Storm to Devices
- [ ] Build on `StormSubmitter`

Topologies
==========
To do realtime computation on Apache Storm for ENOW, we create what are called "topologies". A topology is a graph of computation. Each node in a topology contains processing logic, and links between nodes indicate how data should be passed around between nodes.

TriggerTopology
---------------
TriggerTopology literally can activate the component of Road-Map so that IoT devices are connected each other. before ordering devices, TriggerTopology refine the information of event and link refined one to database for using.

### IndexingBolt :

__INPUT:__
- `eventKafka` ⇨ `jsonObject(Event)`
- `orderKafka` ⇨ `jsonObject(Order)`
- `proceedKafka` ⇨ `jsonObject(Proceed)`


__`jsonObject(Event)` :__</br>
```JSON
{
    "roadMapId":"1",
}
```

__`jsonObject(orderKafka)` :__</br>
```JSON
{
    "corporationName":enow,
    "serverId":serverId1,
    "brokerId":brokerId1,
    "deviceId":deviceId1,
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
    "lastNode":false,
    "order":false,
    "verified":false,
    "lambda":false
}
```

__PROCESSING:__
- `eventKafka`에서 `Console`로부터 받은 `jsonObject(Event)`를 받아온다.
- `orderKafka`에서 `user device`로부터 받은 `jsonObject(Order)`의 정보를 받아온다.
- `proceedKafka`에서 `ActionTopology`로부터 받은 `jsonObject(Proceed)`를 받아온다.
- `eventKafka`, `proceedKafka`와 `orderKafka`에서 받아온 `jsonObject`가 필요한 모든 `key`값을 갖고 있는지 확인한다.
- `orderKafka`에서 받아온 `jsonObject`는 사용자가 직접 보내준 값이므로 `serverId`,`brokerId`,`deviceId` 값이 `MongoDB`에 등록되어 있는지 확인한다.

__OUTPUT:__

- `jsonObject` ⇨ `StagingBolt`
### StagingBolt :
__INPUT:__
- `IndexingBolt` ⇨  `jsonObject(Event)`
`jsonObject(Order)`
`jsonObject(Proceed)`

__PROCESSING:__
- `jsonObject(Event)`를 받은 경우 `MongoDB`에서 `jsonObject(Event)`의 `roadMapId`와 일치하는 `roadMapId`를 찾아 `initNode`들을 실행한다.

- `jsonObject(Order)`를 받은 경우 `MongoDB`에서 `jsonObject(Order)`의 `roadMapId`와 일치하는 `roadMapId`를 찾아 `initNode` 중 `jsonObject(Order)`의 `deviceId`와 일치하는 `deviceId`를 실행한다.

- `proceedKafka`에서 `jsonObject(Proceed)`를 받은 경우 `MongoDB`에서 `jsonObject(proceed)`의 `roadMapId`와 일치하는 `roadMapId`를 찾은 후  `jsonObject(proceed)`의 `mapId`와 일치하는 `mapId`의 `incomingNode`와 `outingNode`를 `jsonObject(Proceed)`에 할당해준다.

__OUTPUT:__
- `jsonArray` ⇨ `CallingTriggerBolt`

### CallingTriggerBolt :
__INPUT:__
- `StagingBolt` ⇨ `jsonArray`

__PROCESSING:__
- `StagingBolt`에서 받은 `jsonArray`를 `jsonObject`로 바꿔 `Trigger topic`으로 보내준다.

__OUTPUT:__
- `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `Topic : Trigger`

ActionTopology
--------------
ActionTopology schedules the enow server to run a Road-Map scheme and handles the executable source code from console pairing the result to devices and event came from TriggerTopology.

### StatusBolt :
__INPUT:__
- `statusKafka` ⇨ `jsonObject(Status)`

__PROCESSING:__
- 각각의 StatusKafka에서 들어오는 JsonObject들을 Topic별로 Redis에 저장한다.
- Save Each `JsonObjects` came from `StatusKafka` to `Redis`.

__OUTPUT:__
- `jsonObject` ⇨ `ExecutingBolt`

### SchedulingBolt :
__INPUT:__
- `triggerKafka` ⇨ `jsonObject(Trigger)`

__PROCESSING:__
- `order`값이 `False`일 때, 현재 노드가 다수의 `incomingNode`들을 가지면, 각각 노드들의 `payload` 정보를 현재 `jsonObject`가 지닌 `Topic`에 근거하여 `Redis`에서 읽어들여 `jsonObject`에 저장한다.
- `incomingNodes`가 `null`이 아니고, 처음 방문한 `mapId`라면 `Redis`에서 각각 `incomingNodes`에 저장된 값들을 꺼내 `jsonObject`안의 `previousData`에 저장한다.
- 만약 이미 방문한적이 있는 `mapId`라면 `verified`값을 `false`로 전환한다.
- `verified`값이 `true`면 각각의 `Trigger` 토픽에서 쏟아져나오는 `jsonObject`값들을 `Redis`에 저장한다.
- 변경된 `jsonObject`를 `ExecutingBolt`로 `emit` 한다.
- When node needs multiple `previousData`, wait for all of `incomingNodes`

__OUTPUT:__
- `jsonObject` ⇨ `ExecutingBolt`

### ExecutingBolt :
__INPUT:__
- `SchedulingBolt` ⇨ `jsonObject`

__PROCESSING:__
- `MongoDB`에서 Source Code 와 Parameter를 받아온다.

__OUTPUT:__
- `jsonObject` ⇨ `ProvisioningBolt`

### ProvisioningBolt :
__INPUT:__
- `ExecutingBolt` ⇨ `jsonObject`

__PROCESSING:__
- 갱신된 `result`를 해당 노드에 매칭된 `Redis`에 갱신한다.

__OUTPUT:__
- `jsonObject` ⇨ `CallingFeedBolt`

### CallingFeedBolt :
__INPUT:__
- `ProvisioningBolt` ⇨ `jsonObject`

__PROCESSING:__
- `ProvisioningBolt`를 참고하여 `KafkaProducer`를 호출한다.
- `outingNode`의 `MapID`별로 `jsonObject`를 갱신시킨 후 `KafkaProducer`를 호출한다.

__OUTPUT:__
- `jsonObject.toJSONString` ⇨ `KafkaProducer` ⇨ `Topic : Feed`

StatusTopology
--------------

### StatusBolt :
__INPUT:__
- `statusKafka` ⇨ `jsonObject(Status)`

__PROCESSING:__
- `Topic`별로 디바이스 정보를 `Redis`에 저장한다.

__OUTPUT:__
- `jsonObject` ⇨ `ExecutingBolt`

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
order is ture : 트리거형 시퀸스의 중간 노드들
order is false : 반복되는 시퀸스
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
