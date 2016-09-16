package com.enow.storm.ActionTopology;

import com.enow.daos.redisDAO.IStatusDAO;
import com.enow.facility.DAOFacility;
import com.enow.persistence.dto.StatusDTO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

/**
 * Created by writtic on 2016. 9. 13..
 */
public class StatusBolt extends BaseRichBolt {
    protected static final Logger _LOG = LogManager.getLogger(ProvisioningBolt.class);
    private OutputCollector _collector;
    private JSONParser _parser;
    private IStatusDAO _dao;

    @Override
    public void prepare(Map MongoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _parser = new JSONParser();
        _dao = DAOFacility.getInstance().createStatusDAO();
    }

    @Override
    public void execute(Tuple input) {

        JSONObject _jsonObject;

        if ((null == input.toString()) || (input.toString().length() == 0)) {
            return;
        }

        String jsonString = input.getValues().toString().substring(1, input.getValues().toString().length() - 1);

        try {
            _jsonObject = (JSONObject) _parser.parse(jsonString);
            _LOG.info("Succeed in inserting messages to _jsonObject : \n" + _jsonObject.toJSONString());
            _dao.addStatus(_dao.jsonObjectToStatus(_jsonObject));
        } catch (ParseException e1) {
            e1.printStackTrace();
            _LOG.warn("Fail in inserting messages to _jsonObject");
            _collector.fail(input);
            return;
        }


        _collector.emit(new Values(input));
        try {
            _LOG.debug("input = [" + input + "]");
            _collector.ack(input);
        } catch (Exception e) {
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}