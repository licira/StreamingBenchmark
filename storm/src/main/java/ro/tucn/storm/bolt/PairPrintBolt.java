package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Created by Liviu on 4/18/2017.
 */
public class PairPrintBolt<T> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(PairPrintBolt.class);

    private boolean windowed;

    public PairPrintBolt() {
        this.windowed = false;
    }

    public PairPrintBolt(boolean windowed) {
        this.windowed = windowed;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (windowed) {
            logger.warn(input.getValue(0).toString()
                    + "\t" + input.getValue(1).toString()
                    + "\t" + input.getValue(2).toString());
        } else {
            logger.warn(input.getValue(0).toString()
                    + "\t" + input.getValue(1).toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
