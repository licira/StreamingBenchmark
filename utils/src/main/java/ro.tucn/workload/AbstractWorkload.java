package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.statistics.PerformanceLog;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by Liviu on 4/9/2017.
 */
public abstract class AbstractWorkload implements Serializable {

    protected static final String TOPIC_ONE_PROPERTY_NAME = "topic1";
    protected static final String TOPIC_TWO_PROPERTY_NAME = "topic2";
    private static final Logger logger = Logger.getLogger(AbstractWorkload.class.getSimpleName());
    private final String dataMode;
    protected PerformanceLog performanceLog;
    protected String workloadName;
    protected int numberOfEntities; // for batch processing

    protected Properties properties;
    protected int parallelism;
    private ContextCreator contextCreator;
    private String frameworkName;

    public AbstractWorkload(ContextCreator contextCreator) throws WorkloadException {
        this.contextCreator = contextCreator;
        this.frameworkName = contextCreator.getFrameworkName();
        this.dataMode = contextCreator.getDataMode();
        this.performanceLog = PerformanceLog.getLogger(this.getClass().getSimpleName());
        this.parallelism = contextCreator.getParallelism();
        initializeProperties();
    }

    public abstract void process();

    public void Start() {
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            process();
            //Works for measuring batch processing performance ONLY

            /*for(int i = 1; i <= 18; i++)
            {
                if(i == 7) parallelism = 2;
                else if (i == 13) parallelism = 4;*/

                PerformanceLog performanceLog = PerformanceLog.getLogger(this.getClass().getSimpleName());
                performanceLog.setStartTime(System.nanoTime());

                contextCreator.start();

                performanceLog.logLatency(System.nanoTime());
                performanceLog.logTotalLatency();
                performanceLog.logToCsv(frameworkName, workloadName, dataMode, performanceLog.getTotalLatency().toString(), numberOfEntities, parallelism);
                logger.info(performanceLog.getTotalLatency());
            //}
        } catch (Exception e) {
            logger.error("WorkloadException caught when trying to run workload " + this.getClass().getSimpleName()
                    + ": " + e.getClass() + " " + e.getMessage());
        }
        logger.info("The end of workload: " + this.getClass().getSimpleName());
    }

    private void initializeProperties() throws WorkloadException {
        properties = new Properties();
        String configFile = this.getClass().getSimpleName() + ".properties";
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + configFile + " failed");
        } catch (Exception e) {
            logger.error("Read configure file: " + configFile + " failed");
        }
    }

    public void setNumberOfEntities(int numberOfEntities) {
        this.numberOfEntities = numberOfEntities;
    }
}