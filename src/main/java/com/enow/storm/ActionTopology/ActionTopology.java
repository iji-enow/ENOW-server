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

        String zkConnString = "localhost:2181";
        String topicTrigger = "trigger";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        SpoutConfig triggerConfig = new SpoutConfig(brokerHosts,topicTrigger, "/"+topicTrigger, "storm");

        triggerConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        triggerConfig.startOffsetTime = -1;

        String topicStatus = "status";

        SpoutConfig statusConfig = new SpoutConfig(brokerHosts,topicStatus, "/"+topicStatus, "storm");

        statusConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        statusConfig.startOffsetTime = -1;

        String topicProceed = "proceed";


        SpoutConfig proceedConfig = new SpoutConfig(brokerHosts,topicProceed, "/"+topicProceed, "storm");

        proceedConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        proceedConfig.startOffsetTime = -1;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setSpout("proceed-spout", new KafkaSpout(proceedConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .allGrouping("trigger-spout")
                .allGrouping("status-spout")
                .allGrouping("proceed-spout");
        builder.setBolt("execute-code-bolt", new ExecuteCodeBolt()).allGrouping("scheduling-bolt");
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).allGrouping("execute-code-bolt");
        builder.setBolt("calling-feed-bolt", new CallingFeedBolt()).allGrouping("provisioning-bolt");
        builder.setBolt("calling-proceed-bolt", new CallingProceedBolt()).allGrouping("provisioning-bolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("ActionTopology", config, builder.createTopology());
    }
}