package ro.tucn.statistics;

import org.apache.log4j.Logger;
import ro.tucn.util.TimeHolder;

/**
 * Created by Liviu on 4/15/2017.
 */
public class PerformanceLog {

    private static PerformanceLog singleton;
    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    private ThroughputLog throughputLog;
    private LatencyLog latencyLog;

    private PerformanceLog(String name) {
        throughputLog = new ThroughputLog(name);
        throughputLog.enablePrint();
        latencyLog = new LatencyLog(name);
        latencyLog.enablePrint();
    }

    public static PerformanceLog getLogger(String name) {
        if (singleton == null) {
            singleton = new PerformanceLog(name);
        } else {
            singleton.setName(name);
        }
        return singleton;
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void logThroughput() {
        throughputLog.execute();
    }

    public <T> void logLatency(TimeHolder<T> TimeHolder) {
        latencyLog.execute(TimeHolder);
    }

    public void logThroughputAndLatency(Long time) {
        throughputLog.execute();
        latencyLog.execute(time);
    }

    public void logThroughputAndLatencyTimeHolder(TimeHolder<?> value) {
        throughputLog.execute();
        latencyLog.execute(value);
    }

    public void logTotalThroughputAndTotalLatency() {
        throughputLog.logTotal();
        latencyLog.logTotal();
    }

    public void setStartTime(Long startTime) {
        throughputLog.setStartTime(startTime);
        throughputLog.setPrevTime(startTime);
        latencyLog.setStartTime(startTime);
        latencyLog.setPrevTime(startTime);
    }

    public void setPrevTime(Long prevTime) {
        throughputLog.setPrevTime(prevTime);
        latencyLog.setPrevTime(prevTime);
    }

    public void enablePrint() {
        throughputLog.disablePrint();
        latencyLog.disablePrint();
    }

    public void disablePrint() {
        throughputLog.disablePrint();
        latencyLog.disablePrint();
    }

    public void setName(String name) {
        throughputLog.setName(name);
        latencyLog.setName(name);
    }
}
