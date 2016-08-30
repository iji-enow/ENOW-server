package com.enow.storm.TriggerTopology;

import org.apache.kafka.clients.producer.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class CallingKafkaBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(CallingKafkaBolt.class);
    private OutputCollector collector;
    private Properties props;
    private Producer<String, String> producer;
    private TopicStructure ts;

    @Override

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        props = new Properties();
        props.put("producer.type", "sync");
        props.put("batch.size", "1");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        ts = new TopicStructure();
    }

    @Override
    public void execute(Tuple input) {
        if (null == input.getValueByField("topicStructure")) {
            return;
        } else if ((null == input.getStringByField("msg") || input.getStringByField("msg").length() == 0)) {
            return;
        }

        ts = (TopicStructure) input.getValueByField("topicStructure");
        final String msg = input.getStringByField("msg");
        final boolean machineIdCheck = input.getBooleanByField("machineIdCheck");
        final boolean phaseRoadMapIdCheck = input.getBooleanByField("phaseRoadMapIdCheck");

        if (machineIdCheck && phaseRoadMapIdCheck) {
            ProducerRecord<String, String> data = new ProducerRecord<String, String>("trigger", ts.output() + " msg : " + msg);
            producer.send(data);
        }

        try {
            LOG.debug("input = [" + input + "]");
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}