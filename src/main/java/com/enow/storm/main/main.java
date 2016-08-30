package com.enow.storm.main;

/**
 * Created by writtic on 2016. 8. 30..
 */
import com.enow.storm.ActionTopology.*;
import com.enow.storm.TriggerTopology.*;

import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class main {
    private static final String[] STREAMS = new String[]{"test_stream","test1_stream","test2_stream"};
    private static final String[] TOPICS = new String[]{"test","test1","test2"};


    public static void main(String[] args) throws Exception {
        new main().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {


        if (args.length == 0) {
            submitTopologyLocalCluster(getTriggerTopolgy(args), getConfig());
            submitTopologyLocalCluster(getActionTopolgy(args), getConfig());
        } else {
            submitTopologyRemoteCluster(args[0], getTriggerTopolgy(args), getConfig());
            submitTopologyRemoteCluster(args[0], getActionTopolgy(args), getConfig());
        }

    }

    protected void submitTopologyLocalCluster(StormTopology topology, Config config) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        stopWaitingForInput();
    }

    protected void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    protected void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected Config getConfig(String zkhost) {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTriggerTopolgy(String[] args) {
        BrokerHosts hosts = new ZkHosts(args[1]);
        TopologyBuilder builder = new TopologyBuilder();
        // event spouts setting
        SpoutConfig eventConfig = new SpoutConfig(hosts, "event", "/event", "enow");
        eventConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        eventConfig.startOffsetTime = -1;
        builder.setSpout("trigger-spout", new KafkaSpout(eventConfig));
        builder.setBolt("indexing-bolt", new IndexingBolt()).allGrouping("event-spout");
        builder.setBolt("staging-bolt", new StagingBolt()).allGrouping("indexing-bolt");
        builder.setBolt("calling-kafka-bolt", new com.enow.storm.ActionTopology.CallingKafkaBolt()).allGrouping("staging-bolt");
        return builder.createTopology();
    }
    protected StormTopology getActionTopolgy(String[] args) {
        BrokerHosts hosts = new ZkHosts(args[1]);
        TopologyBuilder builder = new TopologyBuilder();
        // trigger spouts setting
        SpoutConfig triggerConfig = new SpoutConfig(hosts, "trigger", "/trigger", "enow");
        triggerConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        triggerConfig.startOffsetTime = -1;
        // status spouts setting
        SpoutConfig statusConfig = new SpoutConfig(hosts, "status", "/status", "enow");
        statusConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        statusConfig.startOffsetTime = -1;
        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .allGrouping("trigger-spout")
                .allGrouping("status-spout");
        builder.setBolt("execute-code-bolt", new ExecuteCodeBolt())
                .allGrouping("scheduling-bolt");
        builder.setBolt("provisioning-bolt", new ProvisioningBolt())
                .allGrouping("execute-code-bolt");
        builder.setBolt("calling-kafka-bolt", new com.enow.storm.TriggerTopology.CallingKafkaBolt())
                .allGrouping("provisioning-bolt");
        return builder.createTopology();
    }
}
