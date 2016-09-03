package com.enow.storm.ActionTopology;

import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;


public class ActionTopology {
    public static void main(String[] args) throws Exception {
    	BasicConfigurator.configure();
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        String zkConnString1 = "localhost:2181";
        String topic1 = "trigger";
        BrokerHosts brokerHosts1 = new ZkHosts(zkConnString1);

        SpoutConfig kafkaConfig1 = new SpoutConfig(brokerHosts1,topic1, "/"+topic1, "storm");
        
        kafkaConfig1.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig1.startOffsetTime = -1;
        
        String zkConnString2 = "localhost:2181";
        String topic2 = "status";
        BrokerHosts brokerHosts2 = new ZkHosts(zkConnString2);

        SpoutConfig kafkaConfig2 = new SpoutConfig(brokerHosts1,topic2, "/"+topic2, "storm");

        kafkaConfig2.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig2.startOffsetTime = -1;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("trigger-spout", new KafkaSpout(kafkaConfig1));
        builder.setSpout("status-spout", new KafkaSpout(kafkaConfig2));
        builder.setBolt("scheduling-bolt", new SchedulingBolt()).allGrouping("trigger-spout").allGrouping("status-spout");
        builder.setBolt("execute-code-bolt", new ExecuteCodeBolt()).allGrouping("scheduling-bolt");
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).allGrouping("execute-code-bolt");
        builder.setBolt("calling-kafka-bolt", new CallingFeedBolt()).allGrouping("provisioning-bolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("ActionTopology", config, builder.createTopology());
    }
}