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

import com.enow.storm.mapper.mongodb.SimpleMongoMapper;
import com.enow.storm.mapper.mongodb.mongoDBMapper;

import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.mongodb.common.mapper.MongoMapper;


import java.util.Arrays;

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
    	
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "localhost:2181";
        String topic = "test";
        BrokerHosts brokerHosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,topic, "/"+topic, "storm");
       
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.startOffsetTime = -1;

        String url = "mongodb://127.0.0.1:27017/enow";
        String collectionName = "log";

        
        
        //MongoMapper mapper = new mongoDBMapper().withFields("topicStructure");
        MongoMapper mapper = new SimpleMongoMapper().withFields("topicStructure");

        InsertMongoBolt insertBolt = new InsertMongoBolt(url, collectionName, mapper);
        
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("read-write-mongo-bolt", new ReadWriteMongoDBBolt()).allGrouping("kafka-spout");
        builder.setBolt("insert-bolt", insertBolt).allGrouping("read-write-mongo-bolt");
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
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        
        //Thread.sleep(600000);

    }
}

