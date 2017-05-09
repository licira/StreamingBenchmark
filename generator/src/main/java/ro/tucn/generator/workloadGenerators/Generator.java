package ro.tucn.generator.workloadGenerators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import ro.tucn.generator.producer.ProducerCreator;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.util.ConfigReader;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/5/2017.
 */
public abstract class Generator<K, V> {

    protected static final Logger logger = Logger.getLogger(Generator.class);

    protected ProducerCreator producerCreator = new ProducerCreator();
    protected PerformanceLog performanceLog = PerformanceLog.getLogger(this.getClass().getSimpleName());
    protected ConfigReader configReader = new ConfigReader();

    protected static KafkaProducer<String, String> producer;

    protected Properties properties;
    protected String bootstrapServers;
    protected String TOPIC;

    public Generator() {
        try {
            properties = ConfigReader.getPropertiesFromResourcesFile(this.getClass().getSimpleName() + ".properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        initialzeBootstrapServersData();
    }

    public abstract void generate(int sleepFrequency);

    public void send(String topic, K key, V value) {
        send(topic, key, value);
    }

    protected abstract void generateData(int sleepFrequency);

    protected abstract void initialize();

    protected abstract void initializeTopic();

    protected abstract void initializeDataGenerators();

    protected abstract void initializeWorkloadData();

    private void initialzeBootstrapServersData() {
        Properties properties = null;
        try {
            properties = configReader.getPropertiesFromResourcesFile("DefaultBroker.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        bootstrapServers = properties.getProperty("bootstrap.servers");
    }

    /*protected static void send(String topic, String key, String value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new ProducerRecord(topic, null, timestamp, key, value);
        producer.send(newRecord);
        logger.info("Topic: " + topic
                + "\tTimestamp: " + newRecord.timestamp()
                + "\tValue: " + newRecord.value());
    }*/

    protected void initializeSmallBufferProducer() {
        producer = producerCreator.createSmallBufferProducer(bootstrapServers);
    }

    protected void initializeLargeBufferProducer() {
        producer = producerCreator.createLargeBufferProducer(bootstrapServers);
    }

    protected void initializePerformanceLogWithCurrentTime() {
        Long startTime = System.nanoTime();
        performanceLog.setStartTime(startTime);
        performanceLog.setPrevTime(startTime);
    }
}