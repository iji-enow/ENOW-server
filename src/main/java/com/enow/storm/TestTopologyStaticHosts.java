package com.enow.storm;

import org.apache.log4j.BasicConfigurator;

/**
 * Created by writtic on 2016. 8. 15..
 */

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;

import java.util.ArrayList;
import java.util.List;

public class TestTopologyStaticHosts {


    public static class PrinterBolt extends BaseBasicBolt {

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.toString());
        }

    }

    public static void main(String[] args) throws Exception {
    	BasicConfigurator.configure();
    	/*
        GlobalPartitionInformation hostsAndPartitions = new GlobalPartitionInformation("test");
        hostsAndPartitions.addPartition(0, new Broker("localhost", 9092));
        BrokerHosts brokerHosts = new StaticHosts(hostsAndPartitions);
        */

        String zkConnString = "192.168.99.100:2181";
        String topic = "test";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "/"+topic, "storm");

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.startOffsetTime = -1;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("read-write-mongo-bolt", new ReadWriteMongoDBBolt()).allGrouping("kafka-spout");
        //builder.setBolt("execute-bolt", new ExecuteBolt()).allGrouping("read-write-mongo-bolt");
        //builder.setBolt("python-bolt", new PythonBolt()).allGrouping("execute-bolt");
        //builder.setBolt("read-mongo-bolt", new ReadMongoDBBolt()).allGrouping("write-mongo-bolt");
        //builder.setBolt("read-mongo-bolt", new ReadMongoDBBolt()).allGrouping("write-mongo-bolt");
        //builder.setBolt("write-mongo-bolt", new WriteMongoDBBolt()).allGrouping("read-mongo-bolt");
        builder.setBolt("kafka-bolt", new KafkaSpoutTestBolt()).allGrouping("read-write-mongo-bolt");
        //builder.setBolt("kafka-bolt", new KafkaSpoutTestBolt()).allGrouping("execute-bolt");


        //builder.setBolt("print", new PrinterBolt()).shuffleGrouping("words");
        /*
        if (args != null && args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
            StormSubmitter.submitTopology(name, config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
        }
        */
        Config config = new Config();
        List<String> nimbus_seeds = new ArrayList<String>();
        // nimbus url
        nimbus_seeds.add("192.168.99.100");
        // zookeeper url
        List<String> zookeeper_servers = new ArrayList<String>();
        zookeeper_servers.add("192.168.99.100");
        config.put(Config.NIMBUS_SEEDS, nimbus_seeds);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, zookeeper_servers);
        config.setDebug(true);
        config.setNumWorkers(5);
        StormSubmitter.submitTopology("test", config, builder.createTopology());

        //Thread.sleep(600000);

    }
}
