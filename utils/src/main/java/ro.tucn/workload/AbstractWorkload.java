package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.util.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by Liviu on 4/9/2017.
 */
public abstract class AbstractWorkload implements Serializable {

    private static final Logger logger = Logger.getLogger(AbstractWorkload.class.getSimpleName());

    protected PerformanceLog performanceLog;
    protected String workloadName;
    protected static final String TOPIC_ONE_PROPERTY_NAME = "topic1";
    protected static final String TOPIC_TWO_PROPERTY_NAME = "topic2";
    protected int numberOfEntities; // for batch processing

    protected Properties properties;
    protected int parallelism;
    private ContextCreator contextCreator;

    public AbstractWorkload(ContextCreator contextCreator) throws WorkloadException {
        this.contextCreator = contextCreator;
        this.performanceLog = PerformanceLog.getLogger(this.getClass().getSimpleName());
        initializeParallelism();
        initializeProperties();
    }

    public abstract void process();

    public void Start() {
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            process();
            contextCreator.Start();
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
            //int hosts = Integer.parseInt(properties.getProperty("hosts"));
            //int cores = Integer.parseInt(properties.getProperty("cores"));
        } catch (IOException e) {
            throw new WorkloadException("Read configure file " + configFile + " failed");
        } catch (Exception e) {
            logger.error("Read configure file: " + configFile + " failed");
        }
    }

    private void initializeParallelism() throws WorkloadException {
        Configuration.loadConfiguration();
        parallelism = Configuration.clusterHosts * Configuration.hostCores;
    }

    public void setNumberOfEntities(int numberOfEntities) {
        this.numberOfEntities = numberOfEntities;
    }
}