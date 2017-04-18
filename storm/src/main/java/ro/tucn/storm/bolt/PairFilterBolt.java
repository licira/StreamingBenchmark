package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;
import scala.Tuple2;

/**
 * Created by Liviu on 4/18/2017.
 */
public class PairFilterBolt<K, V> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(FilterBolt.class);

    private FilterFunction<Tuple2<K, V>> fun;

    public PairFilterBolt(FilterFunction<Tuple2<K, V>> function) {
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        Object k = input.getValue(0);
        Object v = input.getValue(1);
        try {
            if (this.fun.filter(new Tuple2<>((K) k, (V) v))) {
                collector.emit(new Values(k, v));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple failed");
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("execute exception: " + e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
