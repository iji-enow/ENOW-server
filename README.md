TEST-storm ![travis](https://travis-ci.org/ENOW-IJI/storm.svg?branch=master)
=========================

FYI, We are using latest version of Apache Storm <br>
Master Branch: [![Travis CI](https://travis-ci.org/apache/storm.svg?branch=master)](https://travis-ci.org/apache/storm)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.storm/storm-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.storm"%20AND%20a:"storm-core")

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

- `triggerKafka`에서 `jsonObject`를 받아 스케줄링을 해준다.
- `ack = false`일 때, 새로 들어온 `mapId`인지 확인하고, 새로 들어온 `mapId`면 `ConcurrentHashMap`에 `peer`들과 함께 저장된다.(`peer`중 하나라도 자신을 저장을 초래한 적이 있다면 무시된다)
- `ack = true`일 때, `ConcurrentHashMap`에 자신과 함께 저장된 `peer`들의 `Key`값을 확인하고, `peer` 들의 `value`값이 모두 `true`면 디바이스에서 `ack` 값을 보낼 준비를 한다.

###### ExecuteBolt :

- `SchdulingBolt`에서 받은 `message` 값과 console에서 설정한 `parameter`값으로 console에서 작성한 `source code`를 실행시킨다.
- `source code`를 실행하고 산출된 `result`를 `SchedulingBolt`에서 받은 토픽과 함께 `provisioningBolt`로 `emit`한다.

###### ProvisioningBolt :

- `proceed`값을 확인하여 `ack` 값을 `false`로 바꿀지 `true`로 바꿀지 결정한다.
- 만약 해당 `mapId`의 `peerOut`값이 없다면 비어있는 채 `CallingFeedBolt`로 결과만 넘겨주고 `peerOut`값이 있다면 `ExecuteBolt`에서 받은 `result`를 messages에 추가하여 `CallingFeedBolt`로 넘겨준다.

###### CallingFeedBolt :

- `peerIn` 이 없는 `Phase`의 init 노드들의 리스트들에 시동을 걸어준다.(시뮬레이션용)
- `ProvisioningBolt`에서 받은 토픽과 메세지를 `feedKafka`로 넘겨준다.

Payload
-------

- __Acknowledge cycle__</br>
```JSON
{
    "corporationName":"enow",
    "serverId":"serverId1",
    "brokerId":"brokerId1",
    "deviceId":"deviceId1",
    "phaseRoadMapId":"1",
    "message":"messages",
    "ack":true,
    "procced":true
}
```

- __Execution cycle__</br>
```JSON
{
    "corporationName":"enow",
    "serverId":"serverId1",
    "brokerId":"brokerId1",
    "deviceId":"deviceId1",
    "phaseRoadMapId":"1",
    "message":"messages",
    "ack":false,
    "procced":false
}
```

References
----------

Test project for enow-storm based on information provided in and referenced by:

- [https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java](https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java)
- [https://github.com/nathanmarz/storm/wiki/Trident-tutorial](https://github.com/nathanmarz/storm/wiki/Trident-tutorial)
- [https://github.com/nathanmarz/storm/wiki/Trident-state](https://github.com/nathanmarz/storm/wiki/Trident-state)
- [https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example)
- [https://github.com/wurstmeister/storm-kafka-0.8-plus-test](https://github.com/wurstmeister/storm-kafka-0.8-plus-test)
