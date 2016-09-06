package com.enow.storm.ActionTopology;

import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;


public class ActionTopology {
    public static void main(String[] args) throws Exception {


        String zkConnString = "localhost:2181";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);
        // Trigger Kafka setting
        String topicTrigger = "trigger";
        SpoutConfig triggerConfig = new SpoutConfig(brokerHosts,topicTrigger, "/"+topicTrigger, "storm");
        triggerConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        triggerConfig.startOffsetTime = -1;
        // Status Kafka setting
        String topicStatus = "status";
        SpoutConfig statusConfig = new SpoutConfig(brokerHosts,topicStatus, "/"+topicStatus, "storm");
        statusConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        statusConfig.startOffsetTime = -1;
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .allGrouping("trigger-spout")
                .allGrouping("status-spout");
        builder.setBolt("execute-code-bolt", new ExecuteCodeBolt()).allGrouping("scheduling-bolt");
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).allGrouping("execute-code-bolt");
        builder.setBolt("calling-feed-bolt", new CallingFeedBolt()).allGrouping("provisioning-bolt");
        LocalCluster cluster = new LocalCluster();

        //BasicConfigurator.configure();
        // Submit Topology to storm nimbus
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        cluster.submitTopology("ActionTopology", config, builder.createTopology());
    }
}