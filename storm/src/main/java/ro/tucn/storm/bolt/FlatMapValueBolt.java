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
public class FlatMapValueBolt<V, R> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(FlatMapValueBolt.class);

    private FlatMapFunction<V, R> fun;

    public FlatMapValueBolt(FlatMapFunction<V, R> function) {
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        Object o = input.getValue(1);
        try {
            Iterable<R> results = (Iterable<R>) this.fun.flatMap((V) o);
            for (R r : results) {
                collector.emit(new Values(input.getValue(0), r));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[1] failed");
        } catch (Exception e) {
            logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer collector) {
        collector.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
