package ro.tucn.storm.bolt.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.DurationException;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liviu on 4/18/2017.
 */
public class WindowPairMapPartitionBolt<K, V, R> extends WindowedBolt {

    private static final Logger logger = Logger.getLogger(WindowMapBolt.class);

    // each slide has a corresponding List<R>
    private List<List<Tuple2<K, V>>> mapedList;
    private MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun;

    public WindowPairMapPartitionBolt(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> function,
                                      TimeDuration windowDuration,
                                      TimeDuration slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        mapedList = new ArrayList<>(WINDOW_SIZE);
        for (int i = 0; i < WINDOW_SIZE; ++i) {
            mapedList.add(i, new ArrayList<Tuple2<K, V>>());
        }
    }

    /**
     * Map T(t) to R(r) and added it to current slide
     *
     * @param tuple
     */
    @Override
    public void processTuple(Tuple tuple) {
        try {
            List<Tuple2<K, V>> list = mapedList.get(slideInWindow);
            K key = (K) tuple.getValue(0);
            V value = (V) tuple.getValue(1);
            list.add(new Tuple2<>(key, value));
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    /**
     * emit all the data(type R) in the current window to next component
     *
     * @param collector
     */
    @Override
    public void processSlide(BasicOutputCollector collector) {
        try {
            List<Tuple2<K, V>> windowList = new ArrayList<>();
            for (List<Tuple2<K, V>> list : mapedList) {
                windowList.addAll(list);
            }
            Iterable<Tuple2<K, R>> list = fun.mapPartition(windowList);
            for (Tuple2<K, R> r : list) {
                collector.emit(new Values(slideIndexInBuffer, r._1(), r._2()));
            }
            // clear data
            mapedList.get((slideInWindow + 1) % WINDOW_SIZE).clear();
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
