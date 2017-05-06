package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.util.Configuration;
import ro.tucn.util.WithTime;

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
    private OperatorCreator operatorCreator;

    public Workload(OperatorCreator operatorCreator) throws WorkloadException {
        this.operatorCreator = operatorCreator;
        initializeParallelism();
        initializeProperties();
    }

    public abstract void process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

    public void Start() {
        logger.info("Start workload: " + this.getClass().getSimpleName());
        try {
            process();
            operatorCreator.Start();
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

    protected Operator<String> getStringStreamOperator(String componentId, String topicPropertyName) {
        return operatorCreator.stringStreamFromKafka(properties, topicPropertyName, componentId, parallelism);
    }

    protected Operator<WithTime<String>> getStringStreamWithTimeOperator(String componentId, String topicPropertyName) {
        return operatorCreator.stringStreamFromKafkaWithTime(properties, topicPropertyName, componentId, parallelism);
    }

    protected Operator<Point> getPointStreamOperator(String componentId, String topicPropertyName) {
        return operatorCreator.pointStreamFromKafka(properties, topicPropertyName, componentId, parallelism);
    }
}