package com.enow.storm.main;

/**
 * Created by writtic on 2016. 8. 30..
 */
import com.enow.storm.ActionTopology.*;
import com.enow.storm.TriggerTopology.*;

// storm
import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

// storm-kafka
import org.apache.storm.kafka.*;

// storm-redis

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class main {
    private static final String[] STREAMS = new String[]{"test_stream","test1_stream","test2_stream"};
    private static final String[] TOPICS = new String[]{"event", "trigger", "status", "proceed", "feed"};
    private static final String zkhost = "localhost:2181";
    private LocalCluster cluster = new LocalCluster();
    public static void main(String[] args) throws Exception {
        new main().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {


        if (args.length == 0) {
        	submitTopologyLocalCluster("action", getActionTopolgy(), getConfig());
            submitTopologyLocalCluster("trigger", getTriggerTopolgy(), getConfig());
        }
//        else {
//            submitTopologyRemoteCluster(args[0], getTriggerTopolgy(args), getConfig());
//            submitTopologyRemoteCluster(args[0], getActionTopolgy(args), getConfig());
//        }

    }

    protected void submitTopologyLocalCluster(String name, StormTopology topology, Config config) throws InterruptedException {
        cluster.submitTopology(name, config, topology);
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

    protected StormTopology getTriggerTopolgy() {
    	BasicConfigurator.configure();
        BrokerHosts hosts = new ZkHosts(zkhost);
        TopologyBuilder builder = new TopologyBuilder();
        // event spouts setting
        SpoutConfig eventConfig = new SpoutConfig(hosts,TOPICS[0], "/" + TOPICS[0], "enow");
        eventConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        eventConfig.startOffsetTime = -1;
        builder.setSpout("event-spout", new KafkaSpout(eventConfig));
        builder.setBolt("indexing-bolt", new IndexingBolt()).allGrouping("event-spout");
        builder.setBolt("staging-bolt", new StagingBolt()).allGrouping("indexing-bolt");
        builder.setBolt("calling-trigger-bolt", new CallingTriggerBolt()).allGrouping("staging-bolt");
        return builder.createTopology();
    }
    protected StormTopology getActionTopolgy() {
    	BasicConfigurator.configure();
        BrokerHosts hosts = new ZkHosts(zkhost);
        TopologyBuilder builder = new TopologyBuilder();
        // trigger spouts setting
        SpoutConfig triggerConfig = new SpoutConfig(hosts, TOPICS[1], "/" + TOPICS[1], "enow");
        triggerConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        triggerConfig.startOffsetTime = -1;
        // status spouts setting
        SpoutConfig statusConfig = new SpoutConfig(hosts, TOPICS[2], "/" + TOPICS[2], "enow");
        statusConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        statusConfig.startOffsetTime = -1;
        // proceed spouts setting
        SpoutConfig proceedConfig = new SpoutConfig(hosts, TOPICS[3], "/" + TOPICS[3], "enow");
        proceedConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        proceedConfig.startOffsetTime = -1;

        builder.setSpout("trigger-spout", new KafkaSpout(triggerConfig));
        builder.setSpout("status-spout", new KafkaSpout(statusConfig));
        builder.setSpout("proceed-spout", new KafkaSpout(proceedConfig));
        builder.setBolt("scheduling-bolt", new SchedulingBolt())
                .allGrouping("trigger-spout")
                .allGrouping("status-spout");
        builder.setBolt("execute-code-bolt", new ExecutingBolt()).allGrouping("scheduling-bolt");
        builder.setBolt("provisioning-bolt", new ProvisioningBolt()).allGrouping("execute-code-bolt");
        builder.setBolt("calling-feed-bolt", new CallingFeedBolt()).allGrouping("provisioning-bolt");
        return builder.createTopology();
    }
}
