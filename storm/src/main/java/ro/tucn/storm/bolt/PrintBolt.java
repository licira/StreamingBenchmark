package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Created by Liviu on 4/18/2017.
 */
public class PrintBolt<T> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(PrintBolt.class);

    private boolean windowed;

    public PrintBolt() {
    }

    public PrintBolt(boolean windowed) {
        this.windowed = windowed;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String key = (String) input.getValue(0);
        String value = (String) input.getValue(1);
        logger.info("Key: " + key + " Value: " + value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}