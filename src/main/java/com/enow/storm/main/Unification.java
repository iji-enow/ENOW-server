package com.enow.storm.main;

import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.enow.storm.ActionTopology.CallingFeedBolt;
import com.enow.storm.ActionTopology.ExecutingBolt;
import com.enow.storm.ActionTopology.ProvisioningBolt;
import com.enow.storm.ActionTopology.SchedulingBolt;
import com.enow.storm.ActionTopology.StatusBolt;
import com.enow.storm.TriggerTopology.CallingTriggerBolt;

import com.enow.storm.TriggerTopology.IndexingBolt;

import com.enow.storm.TriggerTopology.StagingBolt;

public class Unification {
    public static void main(String[] args) throws Exception {
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
        
        String topicTrigger = "trigger";
        SpoutConfig triggerConfig = new SpoutConfig(brokerHosts, topicTrigger, "/"+topicTrigger, "storm");
        triggerConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        triggerConfig.startOffsetTime = -1;
        
        String topicStatus = "status";
        SpoutConfig statusConfig = new SpoutConfig(brokerHosts, topicStatus, "/"+topicStatus, "storm");
        statusConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        statusConfig.startOffsetTime = -1;
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("event-spout", new KafkaSpout(eventConfig));
        builder.setSpout("proceed-spout", new KafkaSpout(proceedConfig));
        builder.setSpout("order-spout", new KafkaSpout(orderConfig));
        builder.setBolt("indexing-bolt", new IndexingBolt()).allGrouping("event-spout").allGrouping("proceed-spout").allGrouping("order-spout");
        builder.setBolt("staging-bolt", new StagingBolt()).allGrouping("indexing-bolt");
        builder.setBolt("calling-trigger-bolt", new CallingTriggerBolt()).allGrouping("staging-bolt");

        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .shuffleGrouping("trigger-spout");
        builder.setBolt("status-bolt", new StatusBolt())
                .shuffleGrouping("status-spout");
        builder.setBolt("executing-bolt", new ExecutingBolt()).shuffleGrouping("scheduling-bolt");
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).shuffleGrouping("executing-bolt");
        builder.setBolt("calling-feed-bolt", new CallingFeedBolt()).shuffleGrouping("provisioning-bolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TriggerTopology", config, builder.createTopology());

    }
}
