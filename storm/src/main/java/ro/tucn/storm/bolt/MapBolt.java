package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;

/**
 * Created by Liviu on 4/18/2017.
 */
public class MapBolt<T, R> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(MapBolt.class);

    private MapFunction<T, R> fun;

    public MapBolt(MapFunction<T, R> function) {
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        Object o = input.getValue(0);
        try {
            R result = this.fun.map((T) o);
            collector.emit(new Values(result));
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}