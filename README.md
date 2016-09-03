TEST-storm ![travis](https://travis-ci.org/ENOW-IJI/storm.svg?branch=master) ![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.storm/storm-core/badge.svg)
=========================

Todo List
---------
- [x] ~~Installation Apache Storm~~
- [x] ~~Installation Apache Kafka~~
- [x] ~~Installation Apache Zookeeper~~
- [x] ~~Making Spout, Bolt, Topology in Storm~~
- [x] ~~Kafka Integration for Storm~~
- [x] ~~Making Connection between Ponte and Kafka~~
- [x] ~~Sending a message from Device(or Android) to Android(or Device) via ENOW System~~
- [x] ~~Not using LocalCluster, But using StormSubmitter~~
- [x] ~~Make Event topology~~
- [ ] Make Trigger & Status topology
- [ ] ~~Make queue for Scheduling Bolt~~ Connect Redis for preserving status info
- [x] ~~Create test Document on MongoDB~~
- [ ] Connect Storm to DashBoard
- [ ] Connect Storm to Devices
- [ ] Build on StormSubmitter

Topologies
----------

모든 볼트는 Redis를 통해 상태 정보가 저장된다.

#### TriggerTopology
##### IndexingBolt :

- ```eventKafka```에서 들어온 현 토픽과 메세지 값을 나눠서 ```TopicStructure``` 구조체에 저장한다.
- 로그를 기록하여 몽고DB에 저장한다.
- 파싱된 ```TopicStructure```를 PhasingBolt로 넘겨준다.

###### PhasingBolt :

- 몽고DB 안의 ```DeviceId```와 ```TopicStructure```에 있는 ```DeviceId```를 매칭한다.
- 몽고DB 안의 ```PhaseRoadMapId```와 ```TopicStructure```에 있는 ```PhaseRoadMapId```를 매칭한다.
- 현제 토픽에 해당하는 ```mapId```를 추출하여 토픽에 추가시켜준다.
- ```TopicStructure```와 위에서 거친 검증 값에 따라 ```CallingTriggerBolt```로 넘겨 준다.

###### CallingTriggerBolt :

- StagingBolt에서 토픽 값과 메세지 값을 받아 ```triggerKafka```로 넘겨준다.

#### ActionTopology

###### SchedulingBolt :

- ```triggerKafka``` 혹은 ```proceedKafka```에서 토픽과 메세지를 받아 스케줄링을 해준다.
- ```triggerKafka```에서 받은 토픽과 메세지는 바로 ```ExecuteBolt```로 넘겨준다.
- ```proceedKafka```에서 받은 토픽과 메세지는 해당 ```mapId```의 peerIn값을 몽고DB에서 받아와 현제 들어온 토픽이외의 다른 ```peerIn```값이 있는 경우 waiting을 걸어준다.
- ```statusKafka```에서 받은 토픽과 메시지는 __현제 실행 예정__ 디바이스의 상태정보와 메타데이터를 받아와 ```triggerKafka```의 값과 매칭시켜 code를 ```ExecuteBolt```를 execute시킬지 waiting시킬지 결정한다.

###### ExecuteBolt :

- ```SchdulingBolt```에서 받은 메세지의 값과 console에서 설정한 parameter값으로 console에서 작성한 source를 실행시킨다.
- source를 돌려서 나온 result값을 ```SchedulingBolt```에서 받은 토픽과 함께 provisioningBolt로 넘겨준다.

###### ProvisioningBolt :

- ```ExecuteBolt```에서 받은 토픽의 ```mapId```의 ```peerOut```값을 몽고DB에서 찾아본다.
- 만약 해당 ```mapId```의 ```peerOut```값 없다면 ```CallingFeedBolt```로 결과만 넘겨주고 ```peerOut```값이 있다면 결과를
- ```CallingFeedBolt```로 넘겨주고 찾은 ```peerOut```값을 ```ExecuteBolt```에서 받은 토픽의 마지막에 추가하여
- ```CallingProceedBolt```로 결과값과 함께 넘겨준다.

###### CallingProceedBolt :

- ```ProvisioningBolt```에서 받은 토픽과 메세지를 ```proceedKafka```로 넘겨준다.

###### CallingFeedBolt :

- ```ProvisioningBolt```에서 받은 토픽과 메세지를 ```feedKafka```로 넘겨준다.


References
----------

Test project for enow-storm based on information provided in and referenced by:

- [https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java](https://github.com/nathanmarz/storm-contrib/blob/master/storm-kafka/src/jvm/storm/kafka/TestTopology.java)
- [https://github.com/nathanmarz/storm/wiki/Trident-tutorial](https://github.com/nathanmarz/storm/wiki/Trident-tutorial)
- [https://github.com/nathanmarz/storm/wiki/Trident-state](https://github.com/nathanmarz/storm/wiki/Trident-state)
- [https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example)
- [https://github.com/wurstmeister/storm-kafka-0.8-plus-test](https://github.com/wurstmeister/storm-kafka-0.8-plus-test)
