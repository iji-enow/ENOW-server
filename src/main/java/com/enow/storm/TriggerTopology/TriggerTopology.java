package com.enow.storm.TriggerTopology;


import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import com.enow.storm.mapper.mongodb.BatchHelper;


import com.enow.storm.mapper.mongodb.InsertMongoBolt;
import com.enow.storm.mapper.mongodb.SimpleMongoMapper;
import com.enow.storm.mapper.mongodb.mongoDBMapper;


public class TriggerTopology {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "localhost:2181";
        String topic = "event";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "/" + topic, "storm");

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.startOffsetTime = -1;
        
        String url = "mongodb://127.0.0.1:27017/log";

        MongoMapper indexingMapper = new SimpleMongoMapper().withFields("spoutSource","topicStructure");
        InsertMongoBolt indexingDBBolt = new InsertMongoBolt(url, "indexingBolt", indexingMapper);
        
        MongoMapper stagingMapper = new SimpleMongoMapper().withFields("spoutSource","topicStructureArray","serverIdCheck" ,"brokerIdCheck","deviceIdCheck", "phaseRoadMapIdCheck", "mapIdCheck");
        InsertMongoBolt stagingDBBolt = new InsertMongoBolt(url, "stagingBolt", stagingMapper);
        
        MongoMapper callingTriggerMapper = new SimpleMongoMapper().withFields("triggerTopologyResult");
        InsertMongoBolt callingTriggerDBBolt = new InsertMongoBolt(url, "callingTriggerBolt", callingTriggerMapper);
        

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("event-spout", new KafkaSpout(kafkaConfig));
        builder.setBolt("indexing-bolt", new IndexingBolt()).allGrouping("event-spout");
        //builder.setBolt("indexing-db-bolt", indexingDBBolt).allGrouping("indexing-bolt");
        builder.setBolt("staging-bolt", new StagingBolt2()).allGrouping("indexing-bolt");
        //builder.setBolt("staging-db-bolt", stagingDBBolt).allGrouping("staging-bolt");
        builder.setBolt("calling-trigger-bolt", new CallingTriggerBolt()).allGrouping("staging-bolt");
        //builder.setBolt("calling-trigger-db-bolt", callingTriggerDBBolt).allGrouping("calling-trigger-bolt");

        /*
        config.setNumWorkers(2);
        config.setMaxTaskParallelism(5);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        
        System.setProperty("storm.jar", "/usr/local/Cellar/storm/1.0.1/libexec/extlib/enow-storm-1.0.jar");
        
        StormSubmitter submitter = new StormSubmitter();
       
        submitter.submitTopology("Trigger", config, builder.createTopology());
        */

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TriggerTopology", config, builder.createTopology());

    }
}

