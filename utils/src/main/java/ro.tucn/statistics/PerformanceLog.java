package ro.tucn.statistics;

import org.apache.log4j.Logger;

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
        }
        return singleton;
    }

    public void logThroughputAndLatency(Long time) {
        throughputLog.execute();
        latencyLog.execute(time);
    }

    public void info(String msg) {
        logger.info(msg);
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
}
