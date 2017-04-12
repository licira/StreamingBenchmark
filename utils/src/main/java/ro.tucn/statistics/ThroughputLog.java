package ro.tucn.statistics;

import org.apache.log4j.Logger;

import ro.tucn.util.Configuration;

import java.io.Serializable;

/**
 * Created by Liviu on 4/6/2017.
 */
public class ThroughputLog implements Serializable {

    private static Logger logger = Logger.getLogger(ThroughputLog.class.getSimpleName());

    private String loggerName;
    private long received;

    private long lastLogTime;
    private long lastLogEle;

    public ThroughputLog(String loggerName) {
        this.loggerName = loggerName;
        this.received = 0;
        this.lastLogTime = 0;
    }

    public void execute() {
        if (Configuration.throughputFrequency != null
                && Configuration.throughputFrequency > 0) {
            execute(Configuration.throughputFrequency);
        } else {
            execute(500);
        }
    }

    public void execute(int logFrequency) {
        long now = System.currentTimeMillis();
        received++;
        if (0 == lastLogTime) {
            this.lastLogTime = now;
        }
        long timeDiff = now - lastLogTime;
        if (timeDiff > logFrequency) {
            long elementDiff = received - lastLogEle;
            double ex = (1000 / (double) timeDiff);

            logger.warn(String.format(this.loggerName + ":\t%d ms\t%d elements\t%d elements/s\t",
                    timeDiff,
                    elementDiff,
                    Double.valueOf(elementDiff * ex).longValue()));
            // reinit
            lastLogEle = received;
            lastLogTime = now;
        }
    }
}