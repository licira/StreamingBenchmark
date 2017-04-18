package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import ro.tucn.util.WithTime;

/**
 * Created by Liviu on 4/18/2017.
 */
public class PairLatencyBolt<T> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(PairLatencyBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        WithTime<T> withTime = (WithTime<T>) input.getValue(1);
        performanceLog.logLatency(withTime);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
