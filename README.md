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
- [ ] Connect Apache Storm to Console
- [ ] Connect Apache Storm to Devices
- [ ] Build on `StormSubmitter`

Elements
--------

#### Peers

- peerOut
- peerIn
- virtualPeer

####

Topologies
----------

#### TriggerTopology
##### IndexingBolt :

- `eventKafka` 토픽에서 들어온 `JSON`꼴의 `String`을 `jsonObject`로 바꿔준다.
- `ack = false`일 경우 `HashMap`에 해당 토픽을 저장해놓는다.
- `ack = true`일 경우 `HashMap`에 해당하는 토픽이 있는지 확인 후 있다면 넘겨주고 없다면 무시한다.

###### StagingBolt :

- 현재 노드에 해당하는 `serverId`, `brokerId`, `deviceId`, `phaseRoadMapId`의 값들이 유효한 값인지 검증한다.
- 현재 노드에 해당하는 `mapId`와 `phaseId`를 추출하여 `jsonObject`에 추가시켜준다.
- `ack = true`일 경우 현재 노드의 `peerOut`, `peerIn` 값들을 확인하여 `jsonObject`에 저장한다.
- `ack = true`일 경우 현재 노드의 `phaseLastNode`과 다음 `phase`의 `phaseInitNode` 값들을 확인하여 `jsonObject`에 저장한다.
- `JSON`과 위에서 거친 검증 값에 따라 `CallingTriggerBolt`로 넘겨 준다.

###### CallingTriggerBolt :

- `StagingBolt`에서 넘겨준 `jsonObject`를 받아 `triggerKafka`로 넘겨준다.

#### ActionTopology

###### SchedulingBolt :

- `triggerKafka`에서 `jsonObject`를 받고, `statusKafka`와 스케줄링을 해준다.
- `triggerKafka`에서 받은 데이터로 `phaseRoadMapId/mapId`로 `primaryKey`를 생성 후 `ConcurrentHashMap`에 저장한다.
- `phaseRoadMapId/mapId`이 중복되어 `triggerKafka`에서 넘어오면 무시한다.
- 저장된 `phaseRoadMapId/mapId`이 `statusKafka`의 센서값과 만나면, 센서값을 `jsonObject`에 추가하고 `ExecutingBolt`로 `emit`한다



- ~~`ack = true`일 때, 자신과 함께 저장된 `peer`들의 `Key`값을 `_peerNode`에서 확인하고, `peer` 들이 모두 `_visitedNode`에 저장되어 있다면, `_visitedNode`의 `value`를 `true`로 전환하고, 디바이스에서 `ack` 값을 보낼 준비를 한다.~~
- ~~`ack = false`일 때, 새로 들어온 `mapId`인지 확인하고, 새로 들어온 `mapId`면 `_peerNode`에 `peer`들의 `mapId`와 함께 `String[]`형으로 저장된다.(`peer`중 하나라도 `_peerNode`의 저장을 초래한 적이 있다면 무시된다) 또한 `incomingPeer`값이 존재하면 아이디 값들을 `_visitedNode`에 존재하는 키(`mapId`)값별 `jsonObject`의 `message`를 참조하여 해당 `jsonObject`의 `previousData`에 넣어준다.~~
- ~~모든 작업이 완료되면 수정된 `jsonObject`를 `ExecutingBolt`로 `emit`한다.~~

###### ExecutingBolt :
- `MongoDB`에서 Source Code 와 Parameter를 받아온다.
- ~~`SchdulingBolt`에서 받은 `message` 값과 console에서 설정한 `parameter`값으로 console에서 작성한 `source code`를 실행시킨다.~~
- ~~`source code`를 실행하고 산출된 `result`를 `SchedulingBolt`에서 받은 토픽과 함께 `provisioningBolt`로 `emit`한다.~~

###### ProvisioningBolt :

- `waitingPeer`값을 확인하여, `result`값들 `previousData`로써 `ConcurrentHashMap`에 `phaseRoadMapId/mapId`를 키값으로 저장해둔다.
-
- ~~`proceed`값을 확인하여 `ack` 값을 `false`로 바꿀지 `true`로 바꿀지 결정한다.~~
- ~~만약 해당 `mapId`의 `peerOut`값이 없다면 비어있는 채 `CallingFeedBolt`로 결과만 넘겨주고 `peerOut`값이 있다면 `ExecutingBolt`에서 받은 `payload`를 `message`에 추가하여 `CallingFeedBolt`로 넘겨준다.~~

###### CallingFeedBolt :

- `outingPeer`값을 확인하여, `반복문`을 통해
- `peerIn` 이 없는 `Phase`의 init 노드들의 리스트들에 시동을 걸어준다.(시뮬레이션용)
- `ProvisioningBolt`에서 받은 토픽과 메세지를 `feedKafka`로 넘겨준다.

Payload
-------

- __Cycle__</br>
```JSON
Event
{
    "corporationName":"enow",
    "serverId":"serverId1",
    "brokerId":"brokerId1",
    "deviceId":"deviceId1",
    "phaseRoadMapId":"1",
    "phaseId":"phaseId1",
    "mapId":1,
    "procced":false,
    "waitingPeer":["1", "2"],
    "incomingPeer":null,
    "outingPeer":["11", "13"],
    "subsequentInitPeer":["15"],
    "previousData":[{},{},{}],
    "payload":[]
}
Status
{
  "topic":"enow/serverId1/brokerId1/deviceId1"
}
```
- __ExecutingBolt__</br>
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
- _"previousData"_ : 이전에 실행했던 `ExecutingBolt` 에서의 결과값을 담고 있다. `RESULT N` 에서의 넘버링은 _스택_ 의 규칙을 따르며 넘버링이 클 수록 최근의 결과값이다.
- _"PARAMETER"_ : 실행에 필요한 파라미터를 세팅하여 넘겨준다.

  __i.e)__ -> _"-l --profile"_

- _"PAYLOAD"_ : 실행에 필요한 입력값을 전달해 준다.

  __i.e)__ ->
  _"{
    'led on' : 1,
    'match object' : 'cat'}
  }"_

- _"SOURCE"_ : 실행에 필요한 소스(__PYTHON__)를 전달해 준다.

  __i.e)__ -> _"def eventHandler(event, context, callback):\n\tevent[\"identification\"] = \"modified\"\n\tprint(\"succeed\")"_

  실행에 필요한 소스의 기본형은 다음과 같다.

  __def eventHandler(event, context, callback):__
    
References
----------

Test project for enow-storm based on information provided in and referenced by:

- [https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java](https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java)
- [https://github.com/nathanmarz/storm/wiki/Trident-tutorial](https://github.com/nathanmarz/storm/wiki/Trident-tutorial)
- [https://github.com/nathanmarz/storm/wiki/Trident-state](https://github.com/nathanmarz/storm/wiki/Trident-state)
- [https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example)
- [https://github.com/wurstmeister/storm-kafka-0.8-plus-test](https://github.com/wurstmeister/storm-kafka-0.8-plus-test)
