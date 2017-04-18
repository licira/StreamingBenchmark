package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;

/**
 * Created by Liviu on 4/18/2017.
 */
public class ReduceBolt<T> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(ReduceBolt.class);
    ReduceFunction<T> fun;
    private T currentValue;

    public ReduceBolt(ReduceFunction<T> function) {
        this.fun = function;
        this.currentValue = null;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        Object o = input.getValue(0);
        try {
            if (null != currentValue) {
                currentValue = this.fun.reduce((T) o, currentValue);
            } else {
                currentValue = (T) o;
            }
            collector.emit(new Values(currentValue));
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Reduce error");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
