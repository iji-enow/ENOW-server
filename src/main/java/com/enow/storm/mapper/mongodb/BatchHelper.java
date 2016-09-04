package com.enow.storm.mapper.mongodb;

import java.util.LinkedList;
import java.util.List;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BatchHelper.class);

    private int batchSize = 15000;  //default batch size 15000
    private List<Tuple> tupleBatch;
    private boolean forceFlush = false;
    private OutputCollector collector;

    public BatchHelper(int batchSize, OutputCollector collector) {
        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
        this.collector = collector;
        this.tupleBatch = new LinkedList<>();
    }

    public void fail(Exception e) {
        collector.reportError(e);
        for (Tuple t : tupleBatch) {
            collector.fail(t);
        }
        tupleBatch.clear();
        forceFlush = false;
    }

    public void ack() {
        for (Tuple t : tupleBatch) {
            collector.ack(t);
        }
        tupleBatch.clear();
        forceFlush = false;
    }

    public boolean shouldHandle(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            LOG.debug("TICK received! current batch status [{}/{}]", tupleBatch.size(), batchSize);
            forceFlush = true;
            return false;
        } else {
            return true;
        }
    }

    public void addBatch(Tuple tuple) {
        tupleBatch.add(tuple);
        if (tupleBatch.size() >= batchSize) {
            forceFlush = true;
        }
    }

    public List<Tuple> getBatchTuples() {
        return this.tupleBatch;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public boolean shouldFlush() {
        return forceFlush && !tupleBatch.isEmpty();
    }

}