package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;
import scala.Tuple2;

/**
 * Created by Liviu on 4/18/2017.
 */
public class PairMapBolt<K, V, R> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(MapBolt.class);

    private MapFunction<Tuple2<K, V>, R> fun;

    public PairMapBolt(MapFunction<Tuple2<K, V>, R> function) {
        this.fun = function;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        K key = (K) input.getValue(0);
        V value = (V) input.getValue(1);
        try {
            R result = this.fun.map(new Tuple2<>(key, value));
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
