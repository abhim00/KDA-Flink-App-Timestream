package com.amazonaws.services.kinesisanalytics.operators;

import com.amazonaws.services.timestream.TimestreamPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*Subscription Filter: CallerService = "GLUE"*/

public class LogsToTimestreamPayloadFn extends RichMapFunction<String, TimestreamPoint> {
    private static final Logger LOG = LoggerFactory.getLogger(LogsToTimestreamPayloadFn.class);

    HashMap<String, String> metrics = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    private void parseLine(String logline) {
        String lineSplit[] = logline.split("=");
        for (String partial : lineSplit){
            switch (partial.toLowerCase()) {
                //Can add any additional desired measures/dimensions from log output here
                case "callerservice":
                    metrics.put("callerservice", lineSplit[1].toLowerCase());
                    break;
                case "operation":
                    metrics.put("operation", lineSplit[1].toLowerCase());
                    break;
                case "awsaccountid":
                    metrics.put("awsaccountid", lineSplit[1].toLowerCase());
                    break;
                case "endtime":
                    metrics.put("endtime", lineSplit[1].toLowerCase());
                    break;
                case "time":
                    //LOG.warn("LOG: In time latency case within JSON Payload function");
                    metrics.put("latency", lineSplit[1].replaceAll("[^\\d.]", "").toLowerCase()); //removes millisecs tag from end

            }
        }

    }

    @Override
    public TimestreamPoint map(String textBasedLogString) throws Exception {

        LOG.warn("Incoming Log: "+textBasedLogString);


        Stream<String> individualLogLines = textBasedLogString.lines();
        individualLogLines.forEach(this::parseLine);
        //LOG.warn("Metrics KeySet: "+metrics.keySet().toString());
        //LOG.warn("Mapping Record --> ValueSet: "+ metrics.values().toString());

        //TODO: Set Record Attributes for TimestreamPoint
        //Currently we are only adding dimensions and no measures
        TimestreamPoint record = new TimestreamPoint();
        for (Map.Entry<String, String> entry : metrics.entrySet()) {

            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            //LOG.warn("LOG: Each Key,Value to be added to TimestreamPoint-->"+key+" : "+value);
            switch (key.toLowerCase()) {
                case "endtime":
                    //LOG.warn("In endtime case: adding key: "+key + "; value: "+value);
                    //SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
                    //Date date = sdf.parse(value);
                    //long millis = date.getTime();
                    //record.setTime(millis); //TODO: NEED TO REPLACE THIS WITH ABOVE.. WONT WORK AS IS WITH DUMMY DATA
                    record.setTime(Long.parseLong(value));
                    record.setTimeUnit("MILLISECONDS");
                    break;
                case "latency":
                    //LOG.warn("In latency case: adding key: "+key + "; value: "+value);
                    record.setMeasureName(key);
                    record.setMeasureValue(value);
                    record.setMeasureValueType("DOUBLE");
                    break;
                default:
                    //LOG.warn("In default case: adding key: "+key + "; value: "+value); //only awsacctid, operation, callerservice should be coming through here
                    record.addDimension(key, value);
            }

        }

        return record;
    }

}
