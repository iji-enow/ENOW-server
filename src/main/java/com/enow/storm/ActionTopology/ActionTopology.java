package com.enow.storm.ActionTopology;

import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ActionTopology {
    public static void main(String[] args) throws Exception {
        //PropertyConfigurator.configure("./src/main.java.resources/log4j.properties");
        BasicConfigurator.configure();

        String zkConnString = "localhost:2181";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);
        // Trigger Kafka setting
        String topicTrigger = "trigger";
        SpoutConfig triggerConfig = new SpoutConfig(brokerHosts, topicTrigger, "/"+topicTrigger, "storm");

        triggerConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        triggerConfig.startOffsetTime = -1;
        // Status Kafka setting
        String topicStatus = "status";
        SpoutConfig statusConfig = new SpoutConfig(brokerHosts, topicStatus, "/"+topicStatus, "storm");
        statusConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        statusConfig.startOffsetTime = -1;
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .fieldsGrouping("trigger-spout", new Fields("jsonObject"));
        builder.setBolt("status-bolt", new StatusBolt())
                .fieldsGrouping("status-spout", new Fields("status"));
        builder.setBolt("executing-bolt", new ExecutingBolt()).fieldsGrouping("scheduling-bolt", new Fields("jsonObject"));
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).fieldsGrouping("executing-bolt", new Fields("jsonObject"));
        builder.setBolt("calling-feed-bolt", new CallingFeedBolt()).fieldsGrouping("provisioning-bolt", new Fields("jsonObject"));
        LocalCluster cluster = new LocalCluster();

        //BasicConfigurator.configure();
        // Submit Topology to storm nimbus
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        cluster.submitTopology("ActionTopology", config, builder.createTopology());
    }
}