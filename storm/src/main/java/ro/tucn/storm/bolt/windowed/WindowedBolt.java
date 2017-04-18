package ro.tucn.storm.bolt.windowed;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ro.tucn.exceptions.DurationException;
import ro.tucn.storm.bolt.BaseBolt;
import ro.tucn.storm.bolt.constants.BoltConstants;
import ro.tucn.util.TimeDuration;

import java.util.Map;

/**
 * Created by Liviu on 4/18/2017.
 */
public abstract class WindowedBolt extends BaseBolt {

    // slides id emit to next component 0,1,2
    protected static int BUFFER_SLIDES_NUM = 3;
    // slideIndexInBuffer couldn't be static, each worker should has its own slideIndexInBuffer
    protected int slideIndexInBuffer = 0;
    // Slides number in a window
    protected int WINDOW_SIZE;
    // slide index in the windowed data structure: (0, 1, ..., WINDOW_SIZE-1)
    protected int slideInWindow = 0;
    // topology tick tuple frequency in seconds
    private long TOPOLOGY_TICK_TUPLE_FREQ_SECS;

    public WindowedBolt(TimeDuration windowDuration, TimeDuration slideDuration) throws DurationException {
        long window_tick_frequency_seconds = TimeDuration.getSeconds(windowDuration);
        long slide_tick_frequency_seconds = TimeDuration.getSeconds(slideDuration);
        this.TOPOLOGY_TICK_TUPLE_FREQ_SECS = slide_tick_frequency_seconds;

        // window duration should be multi of slide duration
        if (window_tick_frequency_seconds % slide_tick_frequency_seconds != 0) {
            throw new DurationException("Window duration should be multi times of slide duration.");
        }
        WINDOW_SIZE = (int) (window_tick_frequency_seconds / slide_tick_frequency_seconds);
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (performanceLog != null) {
            performanceLog.setStartTime(System.nanoTime());
            performanceLog.logThroughput();
        }
        if (isTickTuple(tuple)) {
            processSlide(collector);
            collector.emit(BoltConstants.TICK_STREAM_ID, new Values(slideIndexInBuffer));
            slideIndexInBuffer = (slideIndexInBuffer + 1) % BUFFER_SLIDES_NUM;
            slideInWindow = (slideInWindow + 1) % WINDOW_SIZE;
        } else {
            processTuple(tuple);
        }
    }

    public abstract void processTuple(Tuple tuple);

    public abstract void processSlide(BasicOutputCollector collector);

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}