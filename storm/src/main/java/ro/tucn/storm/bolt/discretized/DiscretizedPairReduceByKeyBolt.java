package ro.tucn.storm.bolt.discretized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Liviu on 4/18/2017.
 */
public class DiscretizedPairReduceByKeyBolt<K, V> extends DiscretizedBolt {

    private static final Logger logger = Logger.getLogger(DiscretizedPairReduceByKeyBolt.class);

    private ReduceFunction<V> fun;
    private Map<Integer, Map<K, V>> slideDataMap;

    public DiscretizedPairReduceByKeyBolt(ReduceFunction<V> function, String preComponentId) {
        super(preComponentId);
        this.fun = function;
        slideDataMap = new HashMap<>(BUFFER_SLIDES_NUM);
        for (int i = 0; i < BUFFER_SLIDES_NUM; ++i)
            slideDataMap.put(i, new HashMap<K, V>());
    }

    @Override
    public void processTuple(Tuple tuple) {
        try {
            int slideId = tuple.getInteger(0);
            slideId = slideId % BUFFER_SLIDES_NUM;
            K key = (K) tuple.getValue(1);
            V value = (V) tuple.getValue(2);

            Map<K, V> slideMap = slideDataMap.get(slideId);
            if (null == slideMap) {
                slideMap = new HashMap<>();
                slideMap.put(key, value);
                slideDataMap.put(slideId, slideMap);
            } else {
                V reducedValue = slideMap.get(key);
                if (null == reducedValue) {
                    slideMap.put(key, value);
                } else {
                    slideMap.put(key, fun.reduce(reducedValue, value));
                }
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector, int slideIndex) {
        Map<K, V> slideMap = slideDataMap.get(slideIndex);
        for (Map.Entry<K, V> entry : slideMap.entrySet()) {
            //collector.emit(new Values(slideIndex, entry.getKey(), entry.getValue()));
            collector.emit(new Values(entry.getKey(), entry.getValue()));
        }
        // clear data
        slideMap.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        //declarer.declareStream(Utils.DEFAULT_STREAM_ID,
        //new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                new Fields(BoltConstants.OutputKeyField, BoltConstants.OutputValueField));
    }
}
