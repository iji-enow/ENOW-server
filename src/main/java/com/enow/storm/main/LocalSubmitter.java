package com.enow.storm.main;

import com.enow.persistence.redis.IRedisDB;
import com.enow.persistence.redis.RedisDB;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.enow.storm.ActionTopology.CallingFeedBolt;
import com.enow.storm.ActionTopology.ExecutingBolt;
import com.enow.storm.ActionTopology.ProvisioningBolt;
import com.enow.storm.ActionTopology.SchedulingBolt;
import com.enow.storm.ActionTopology.StatusBolt;
import com.enow.storm.TriggerTopology.CallingTriggerBolt;

import com.enow.storm.TriggerTopology.IndexingBolt;

import com.enow.storm.TriggerTopology.StagingBolt;

public class LocalSubmitter {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        IRedisDB _redis = RedisDB.getInstance("127.0.0.1", 6379);
        config.setDebug(true);
        config.put("mongodb.ip", "127.0.0.1");
        config.put("mongodb.port", 27017);
        config.put("redis.ip", "127.0.0.1");
        config.put("redis.port", 6379);
        config.put("kafka.properties", "127.0.0.1:9092");
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
        builder.setBolt("staging-bolt", new StagingBolt()).fieldsGrouping("indexing-bolt", new Fields("roadMapId"));
        builder.setBolt("calling-trigger-bolt", new CallingTriggerBolt()).fieldsGrouping("staging-bolt", new Fields("roadMapId"));

        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .shuffleGrouping("trigger-spout");
        builder.setBolt("status-bolt", new StatusBolt())
                .shuffleGrouping("status-spout");
        builder.setBolt("executing-bolt", new ExecutingBolt()).fieldsGrouping("scheduling-bolt",new Fields("roadMapId"));
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).fieldsGrouping("executing-bolt",new Fields("roadMapId"));
        builder.setBolt("calling-feed-bolt", new CallingFeedBolt()).fieldsGrouping("provisioning-bolt",new Fields("roadMapId"));
        _redis.deleteAllNodes();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TriggerTopology", config, builder.createTopology());

    }
}
