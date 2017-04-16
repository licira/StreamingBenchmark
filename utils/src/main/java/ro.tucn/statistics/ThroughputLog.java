package ro.tucn.statistics;

import org.apache.log4j.Logger;
import ro.tucn.util.Configuration;
import ro.tucn.util.TimeDuration;

import java.io.Serializable;
import java.sql.Time;

/**
 * Created by Liviu on 4/6/2017.
 */
public class ThroughputLog implements Serializable {

    private static Logger logger = Logger.getLogger(ThroughputLog.class.getSimpleName());

    private static final String THROUGHPUT_MSG = ": Throughput:";
    private static final String TOTAL_THROUGHPUT_MSG = ": Total Throughput:";

    private String name;
    private Long received;
    private Long prevTime;
    private Long lastLogEle;
    private Long startTime;
    private Long totalTimeDiff;
    private Long totalElementDiff;
    private boolean printEnabled;

    public ThroughputLog(String name) {
        this.name = name;
        received = 0L;
        prevTime = 0L;
        lastLogEle = 0L;
        totalElementDiff = 0L;
        totalTimeDiff = 0L;
    }

    public void execute() {
        if (Configuration.throughputFrequencyToBeLogged()) {
            execute(Configuration.throughputFrequency);
        } else {
            execute(500);
        }
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
        prevTime = startTime;
        totalElementDiff = 0L;
        totalTimeDiff = 0L;
    }

    public void setPrevTime(Long prevTime) {
        this.prevTime = prevTime;
    }

    private void execute(int logFrequency) {
        Long now = System.nanoTime();
        received++;
        if (0 == prevTime) {
            this.prevTime = now;
        }
        Long timeDiff = now - prevTime;
        Long elementDiff = received - lastLogEle;
        if (timeDiff > logFrequency) {
            double ex = TimeDuration.nanosToSeconds(timeDiff);
            double latency = TimeDuration.nanosToSeconds(timeDiff);
            long elementsPerSecond = Double.valueOf(elementDiff * ex).longValue();

            log(THROUGHPUT_MSG, latency, elementDiff, elementsPerSecond);
            // reinit
            lastLogEle = received;
            prevTime = now;
            totalTimeDiff += timeDiff;
            totalElementDiff += elementDiff;
        }
    }

    private void log(String msg, double timeDiff, Long elementDiff, long elementsPerSecond) {
        if (printEnabled) {
            logger.info(String.format("%-25s\t%fs\t%delement(s)\t%delement(s)/s\t",
                    name + ": " + msg,
                    timeDiff,
                    elementDiff,
                    elementsPerSecond));
        }
    }

    public void logTotal() {
        double ex = TimeDuration.nanosToSeconds(totalTimeDiff);
        double latency = TimeDuration.nanosToSeconds(totalTimeDiff);
        long elementsPerSecond = Double.valueOf(totalElementDiff * ex).longValue();
        log(TOTAL_THROUGHPUT_MSG, latency, totalElementDiff, elementsPerSecond);
    }

    public void reset() {
        startTime = 0L;
        prevTime = 0L;
        received = 0L;
        totalTimeDiff = 0L;
        totalElementDiff = 0L;
        lastLogEle = 0L;
    }

    public void enablePrint() {
        printEnabled = true;
    }

    public void disablePrint() {
        printEnabled = false;
    }
}