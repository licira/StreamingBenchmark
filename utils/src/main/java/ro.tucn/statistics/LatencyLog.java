package ro.tucn.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tucn.util.Configuration;
import ro.tucn.util.WithTime;

/**
 * Created by Liviu on 4/6/2017.
 */
public class LatencyLog {

    private static Logger logger = LoggerFactory.getLogger(LatencyLog.class);

    private String loggerName;

    public LatencyLog(String loggerName) {
        this.loggerName = loggerName;
    }

    public void execute(WithTime<? extends Object> withTime) {
        long latency = System.currentTimeMillis() - withTime.getTime();

        double frequency = 0.001;
        if (Configuration.latencyFrequency != null
                && Configuration.latencyFrequency > 0) {
            frequency = Configuration.latencyFrequency;
        }

        // probability to log 0.001
        if (Math.random() < frequency) {
            logger.warn(String.format(this.loggerName + ":\t%d", latency));
        }
    }

    public void execute(long time) {
        long latency = System.currentTimeMillis() - time;
        double probability = 0.001;
        if (Configuration.latencyFrequency != null
                && Configuration.latencyFrequency > 0) {
            probability = Configuration.latencyFrequency;
        }
        // probability to log 0.001
        if (Math.random() < probability) {
            logger.warn(String.format(this.loggerName + ":\t%d", latency));
        }
    }
}