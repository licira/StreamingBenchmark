package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Liviu on 4/18/2017.
 */
public class PairReduceBolt<K, V> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(PairReduceBolt.class);

    private Map<K, V> map;
    private ReduceFunction<V> fun;

    public PairReduceBolt(ReduceFunction<V> function) {
        this.fun = function;
        map = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        Object k = input.getValue(0);
        Object v = input.getValue(1);
        V currentValue = map.get(k);
        try {
            K key = (K) k;
            if (null != currentValue) {
                currentValue = this.fun.reduce((V) v, currentValue);
            } else {
                currentValue = (V) v;
            }
            map.put(key, currentValue);
            collector.emit(new Values(key, currentValue));
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Reduce error");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}