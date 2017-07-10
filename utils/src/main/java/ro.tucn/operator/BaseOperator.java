package ro.tucn.operator;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.util.TimeDuration;

import java.io.Serializable;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class BaseOperator implements Serializable {

    protected String dataMode;
    protected String frameworkName;
    protected int parallelism = -1;
    protected long executionLatency;
    protected PerformanceLog performanceLog;
    protected Object performanceListener;
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public BaseOperator(int parallelism) {
        this.setParallelism(parallelism);
        this.performanceLog = PerformanceLog.getLogger(this.getClass().getName());
    }

    public int getParallelism() {
        return this.parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public abstract void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException;

    public abstract void print();

    public void printExecutionLatency() {
        logger.info(String.format("%-25s\t%f sec", "Execution Latency: ", TimeDuration.nanosToSeconds(executionLatency)));
    }

    public long getExecutionLatency() {
        return executionLatency;
    }

    public void setExecutionLatency(long executionLatency) {
        this.executionLatency = executionLatency;
    }

    public String getDataMode() {
        return dataMode;
    }

    public String getFrameworkName() {
        return frameworkName;
    }

    public void setPerformanceListener(Object performanceListener) {
        this.performanceListener = performanceListener;
    }
}