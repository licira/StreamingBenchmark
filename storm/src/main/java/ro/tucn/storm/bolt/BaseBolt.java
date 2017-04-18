package ro.tucn.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.storm.bolt.constants.BoltConstants;

/**
 * Created by Liviu on 4/18/2017.
 */
public abstract class BaseBolt extends BaseBasicBolt {

    protected PerformanceLog performanceLog;

    public void enableThroughput(String name) {
        this.performanceLog = PerformanceLog.getLogger(name);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* declare tick stream, tick tuple with id of the latest end slide */
        declarer.declareStream(BoltConstants.TICK_STREAM_ID, new Fields(BoltConstants.OutputSlideIdField));
    }
}
