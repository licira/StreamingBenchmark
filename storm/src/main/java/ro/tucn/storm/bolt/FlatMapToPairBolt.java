package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.FlatMapPairFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;
import scala.Tuple2;

/**
 * Created by Liviu on 4/18/2017.
 */
public class FlatMapToPairBolt<T, K, V> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(MapToPairBolt.class);

    private FlatMapPairFunction<T, K, V> fun;

    public FlatMapToPairBolt(FlatMapPairFunction<T, K, V> function) {
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
            Iterable<Tuple2<K, V>> results = this.fun.flatMapToPair((T) o);
            for (Tuple2<K, V> tuple2 : results) {
                collector.emit(new Values(tuple2._1(), tuple2._2()));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
