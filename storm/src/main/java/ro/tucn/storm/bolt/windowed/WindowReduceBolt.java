package ro.tucn.storm.bolt.windowed;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.DurationException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.util.TimeDuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liviu on 4/18/2017.
 */
public class WindowReduceBolt<T> extends WindowedBolt {

    private static final Logger logger = Logger.getLogger(WindowReduceBolt.class);

    // window data structure TODO: replace it with tree
    private List<T> reduceList;
    private ReduceFunction<T> fun;

    public WindowReduceBolt(ReduceFunction<T> function, TimeDuration windowDuration, TimeDuration slideDuration) throws DurationException {
        super(windowDuration, slideDuration);
        this.fun = function;
        reduceList = new ArrayList<>(WINDOW_SIZE);
    }
    @Override
    public void processTuple(Tuple tuple) {
        try {
            T reduceValue = reduceList.get(slideInWindow);
            T value = (T) tuple.getValue(0);
            if (null == reduceValue)
                reduceList.set(slideInWindow, value);
            else {
                reduceList.set(slideInWindow, fun.reduce(reduceValue, value));
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void processSlide(BasicOutputCollector collector) {
        try {
            T reduceValue = null;
            // TODO: implement window data structure with tree, no need to for loop
            for (T t : reduceList) {
                if (null == reduceValue) {
                    reduceValue = t;
                } else {
                    reduceValue = fun.reduce(reduceValue, t);
                }
            }
            collector.emit(new Values(slideIndexInBuffer, reduceValue));
            // clear data
            reduceList.set((slideInWindow + 1) % WINDOW_SIZE, null);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields(BoltConstants.OutputSlideIdField, BoltConstants.OutputValueField));
    }
}
