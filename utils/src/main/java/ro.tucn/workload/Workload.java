package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.ContextCreator;
import ro.tucn.util.Configuration;
import ro.tucn.util.TimeHolder;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Created by Liviu on 4/9/2017.
 */
public abstract class Workload implements Serializable {

    private static final Logger logger = Logger.getLogger(Workload.class.getSimpleName());

    protected Properties properties;
    protected int parallelism;
    private ContextCreator ContextCreator;

    public Workload(ContextCreator ContextCreator) throws WorkloadException {
        this.ContextCreator = ContextCreator;
        initializeParallelism();
        initializeProperties();
    }

    public abstract void process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

    public void Start() {
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            process();
            ContextCreator.Start();
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

    protected StreamOperator<TimeHolder<String>> getStringStreamTimeHolderOperator(String componentId, String topicPropertyName) {
        //return ContextCreator.getStringStreamTimeHolderFromKafka(properties, topicPropertyName, componentId, parallelism);
        return null;
    }
}