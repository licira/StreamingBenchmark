package ro.tucn.generator.generator;

import org.apache.log4j.Logger;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.util.ConfigReader;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/5/2017.
 */
public abstract class AbstractGenerator<K, V> {

    protected static final Logger logger = Logger.getLogger(AbstractGenerator.class);

    private static final String BROKER_FILE_PROPERTIES_NAME = "DefaultBroker.properties";

    protected PerformanceLog performanceLog = PerformanceLog.getLogger(this.getClass().getSimpleName());
    protected Properties properties;
    protected String bootstrapServers;
    protected int entitiesNumber;

    public AbstractGenerator(int entitiesNumber) {
        try {
            properties = ConfigReader.getPropertiesFromResourcesFile(this.getClass().getSimpleName() + ".properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        initialzeBootstrapServersData();
        this.entitiesNumber = entitiesNumber;
    }

    public abstract void generate(int sleepFrequency);

    protected abstract void submitData(int sleepFrequency);

    protected abstract void initialize();

    protected abstract void initializeDataGenerators();

    protected abstract void initializeWorkloadData();

    private void initialzeBootstrapServersData() {
        Properties properties = null;
        try {
            properties = ConfigReader.getPropertiesFromResourcesFile(BROKER_FILE_PROPERTIES_NAME);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        bootstrapServers = properties.getProperty("bootstrap.servers");
    }

    protected void initializePerformanceLogWithCurrentTime() {
        Long startTime = System.nanoTime();
        performanceLog.setStartTime(startTime);
        performanceLog.setPrevTime(startTime);
    }

    public void setEntitiesNumber(int entitiesNumber) {
        this.entitiesNumber = entitiesNumber;
    }
}