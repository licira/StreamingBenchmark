package ro.tucn.statistics;

import org.apache.log4j.Logger;
import ro.tucn.util.Configuration;
import ro.tucn.util.TimeDuration;
import ro.tucn.util.WithTime;

import java.io.Serializable;

/**
 * Created by Liviu on 4/6/2017.
 */
public class LatencyLog implements Serializable {

    private static Logger logger = Logger.getLogger(LatencyLog.class.getSimpleName());

    private static final String LATENCY_MSG = ": Latency:";
    private static final String TOTAL_LATENCY_MSG =": Total Latency: ";

    private String name;
    private Long startTime;
    private Long prevTime;
    private Long totalLatency;
    private boolean printEnabled;

    public LatencyLog(String name) {
        this.name = name;
        totalLatency = 0L;
        prevTime = 0L;
    }

    public void execute(WithTime<? extends Object> withTime) {
        Long latency = System.nanoTime() - withTime.getTime();
        double frequency = 0.001;
        if (Configuration.latencyFrequency != null
                && Configuration.latencyFrequency > 0) {
            frequency = Configuration.latencyFrequency;
        }
        // probability to log 0.001
        if (Math.random() < frequency) {
            log("Latency: ", TimeDuration.nanosToSeconds(latency.longValue()));
        }
        totalLatency += latency;
    }

    public void execute(Long time) {
        Long latency = time - prevTime;
        prevTime = time;
        double probability = 0.001;
        if (Configuration.latencyFrequency != null
                && Configuration.latencyFrequency > 0) {
            probability = Configuration.latencyFrequency;
        }
        // probability to log 0.001
        //if (Math.random() < probability)
        {
            log(name + LATENCY_MSG, TimeDuration.nanosToSeconds(latency.longValue()));
        }
        totalLatency += latency;
    }

    private void log(String msg, double latency) {
        if (printEnabled) {
            logger.info(String.format("%-25s\t%fs", msg, latency));
        }
    }

    public void logTotal() {
        log(name + TOTAL_LATENCY_MSG, TimeDuration.nanosToSeconds(totalLatency.longValue()));
    }

    public void reset() {
        startTime = null;
        prevTime = null;
        totalLatency = 0L;
    }

    public void setPrevTime(Long prevTime) {
        this.prevTime = prevTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
        prevTime = startTime;
        totalLatency = 0L;
    }

    public void enablePrint() {
        printEnabled = true;
    }

    public void disablePrint() {
        printEnabled = false;
    }
}