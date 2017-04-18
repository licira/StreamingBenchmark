package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.FlatMapFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;

/**
 * Created by Liviu on 4/18/2017.
 */
public class FlatMapBolt<T, R> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(FlatMapBolt.class);

    private FlatMapFunction<T, R> fun;

    public FlatMapBolt(FlatMapFunction<T, R> function) {
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
            Iterable<R> results = (Iterable<R>) this.fun.flatMap((T) o);
            for (R r : results) {
                collector.emit(new Values(r));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
