package ro.tucn.statistics;

import org.apache.log4j.Logger;
import ro.tucn.util.TimeHolder;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Liviu on 4/15/2017.
 */
public class PerformanceLog {

    private static final String CSV_EXTENSION = ".csv";

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

    public void logToCsv(String frameworkName, String workloadName, String dataMode, long latency, Object throughput) {
        String filename = getFileName(frameworkName, workloadName, dataMode);
        try {
            FileWriter fw = getFileWriter(filename);
            appendToCsv(fw, latency, throughput);
            closeFile(fw);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.info(filename);
    }

    private void closeFile(FileWriter fw) throws IOException {
        fw.flush();
        fw.close();
    }

    private void appendToCsv(FileWriter fw, long latency, Object throughput) throws IOException {
        fw.append("" + latency);
        fw.append(",");
        fw.append("smt else");
        fw.append("\n");
    }

    private FileWriter getFileWriter(String filename) throws IOException {
        FileWriter fw = new FileWriter(filename, true);
        return fw;
    }

    private String getFileName(String frameworkName, String workloadName, String dataMode) {
        return frameworkName + "-" + workloadName + "-" + dataMode + CSV_EXTENSION;
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

    public void logLatency(long time) {
        latencyLog.execute(time);
    }

    public void logThroughputAndLatency(Long time) {
        throughputLog.execute();
        latencyLog.execute(time);
    }

    public void logThroughputAndLatencyTimeHolder(TimeHolder<?> value) {
        throughputLog.execute();
        latencyLog.execute(value);
    }


    public void logTotalLatency() {
        latencyLog.logTotal();
    }

    public void logTotalThroughputAndTotalLatency() {
        throughputLog.logTotal();
        throughputLog.logTotalSize();
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

    public void logSize(Object obj) {
        throughputLog.logSize(obj);
    }

    public Long getTotalLatency() {
        return latencyLog.getTotalLatency();
    }
}
