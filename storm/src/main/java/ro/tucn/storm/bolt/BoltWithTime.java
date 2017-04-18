package ro.tucn.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.util.Constants;
import ro.tucn.util.WithTime;

/**
 * Created by Liviu on 4/18/2017.
 */
public class BoltWithTime<T> extends BaseBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getValue(0) instanceof String) {
            String value = (String) input.getValue(0);
            String[] list = value.split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                collector.emit(new Values(new WithTime<T>((T) list[0], Long.parseLong(list[1]))));
            } else {
                collector.emit(new Values(new WithTime<T>((T) input.getValue(0), System.nanoTime())));
            }
        } else {
            collector.emit(new Values(new WithTime<T>((T) input.getValue(0), System.nanoTime())));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BoltConstants.OutputValueField));
    }
}
