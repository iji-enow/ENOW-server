package com.enow.storm.EventTopology;


import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;


public class EventTopology {
    public static void main(String[] args) throws Exception {
    	BasicConfigurator.configure();
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "localhost:2181";
        String topic = "event";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, "/"+topic, "storm");
       
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.startOffsetTime = -1;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("event-spout", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("indexing-bolt", new IndexingBolt()).allGrouping("event-spout");
        builder.setBolt("staging-bolt", new StagingBolt()).allGrouping("indexing-bolt");
        builder.setBolt("calling-kafka-bolt", new CallingKafkaBolt()).allGrouping("staging-bolt");
        
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Event", config, builder.createTopology());
        
    }
}

