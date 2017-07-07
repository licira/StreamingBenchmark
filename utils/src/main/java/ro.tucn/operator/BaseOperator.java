package ro.tucn.operator;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.statistics.PerformanceLog;

import java.io.Serializable;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class BaseOperator implements Serializable {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    protected int parallelism = -1;
    protected long executionLatency;
    protected PerformanceLog performanceLog;

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
        logger.info(String.format("%-25s\t%d", "Execution Latency: ", executionLatency));
    }

    public void setExecutionLatency(long executionLatency) {
        this.executionLatency = executionLatency;
    }
}