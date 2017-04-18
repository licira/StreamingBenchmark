package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.storm.bolt.constants.BoltConstants;

/**
 * Created by Liviu on 4/18/2017.
 */
public class FilterBolt<T> extends BaseBolt {

    private static final Logger logger = Logger.getLogger(FilterBolt.class);

    private FilterFunction<T> fun;

    public FilterBolt(FilterFunction<T> function) {
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
            if (this.fun.filter((T) o)) {
                collector.emit(new Values(o));
            }
        } catch (ClassCastException e) {
            logger.error("Cast tuple[0] failed");
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("execute exception: " + e.toString());
        }
        logger.error("Hello world");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
