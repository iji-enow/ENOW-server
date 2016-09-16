package com.enow.storm.TriggerTopology;


import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.enow.persistence.mongodb.InsertMongoBolt;
import com.enow.persistence.mongodb.mapper.SimpleMongoMapper;



public class TriggerTopology {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "localhost:2181";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        SpoutConfig eventConfig = new SpoutConfig(brokerHosts, "event", "/event","storm");

        eventConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        eventConfig.startOffsetTime = -1;
        
        SpoutConfig proceedConfig = new SpoutConfig(brokerHosts, "proceed", "/proceed", "storm");

        proceedConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        proceedConfig.startOffsetTime = -1;
        
        SpoutConfig orderConfig = new SpoutConfig(brokerHosts, "order", "/order", "storm");

        orderConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        orderConfig.startOffsetTime = -1;
        
        
        String url = "mongodb://127.0.0.1:27017/log";

        MongoMapper indexingMapper = new SimpleMongoMapper().withFields("jsonObject");
        InsertMongoBolt indexingDBBolt = new InsertMongoBolt(url, "indexingBolt", indexingMapper);
        
        MongoMapper stagingMapper = new SimpleMongoMapper().withFields("jsonArray", "serverIdCheck", "brokerIdCheck", "deviceIdCheck",
				"phaseRoadMapIdCheck", "mapIdCheck");
        InsertMongoBolt stagingDBBolt = new InsertMongoBolt(url, "stagingBolt", stagingMapper);
        
        MongoMapper callingTriggerMapper = new SimpleMongoMapper().withFields("triggerTopologyResult");
        InsertMongoBolt callingTriggerDBBolt = new InsertMongoBolt(url, "callingTriggerBolt", callingTriggerMapper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("event-spout", new KafkaSpout(eventConfig));
        builder.setSpout("proceed-spout", new KafkaSpout(proceedConfig));
        builder.setSpout("order-spout", new KafkaSpout(orderConfig));
        builder.setBolt("indexing-bolt", new IndexingBolt()).allGrouping("event-spout").allGrouping("proceed-spout").allGrouping("order-spout");
        //builder.setBolt("indexing-db-bolt", indexingDBBolt).allGrouping("indexing-bolt");
        builder.setBolt("staging-bolt", new StagingBolt()).allGrouping("indexing-bolt");
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

