package ro.tucn.generator.workloadGenerators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.statistics.PerformanceLog;
import ro.tucn.util.ConfigReader;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/5/2017.
 */
public abstract class Generator {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected PerformanceLog performanceLog = PerformanceLog.getLogger(this.getClass().getSimpleName());
    protected ConfigReader configReader = new ConfigReader();

    protected static KafkaProducer<String, String> producer;
    protected ProducerRecord<String, String> newRecord;
    protected Properties properties;
    protected String bootstrapServersHost;
    protected String bootstrapServersPort;
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

    protected void send(String topic, String key, String value) {
        newRecord = new ProducerRecord<>(topic, null, System.nanoTime(), key, value);
        producer.send(newRecord);
        logger.info("Timestamp: " + newRecord.timestamp() + "\tValue: " + newRecord.value());
    }

    private void initialzeBootstrapServersData() {
        Properties properties = null;
        try {
            properties = configReader.getPropertiesFromResourcesFile("DefaultBroker.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        bootstrapServersHost = properties.getProperty("bootstrap.servers.host");
        bootstrapServersPort = properties.getProperty("bootstrap.servers.port");
    }

    private KafkaProducer<String, String> createSmallBufferProducer() {
        Properties props = getDefaultKafkaProducerProperties();
        return createKafkaProducerWithProperties(props);
    }

    private KafkaProducer<String, String> createLargeBufferProducer() {
        Properties props = getLargeBufferKafkaProducerProperties();
        return createKafkaProducerWithProperties(props);
    }

    private KafkaProducer<String, String> createKafkaProducerWithProperties(Properties props) {
        return new KafkaProducer<String, String>(props);
    }

    private Properties getDefaultKafkaProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServersHost + ":" + bootstrapServersPort);
        //props.put("group.id", "test");
        /*
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        */
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private Properties getLargeBufferKafkaProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersHost + ":" + bootstrapServersPort);
        //props.put("group.id", "test");

        //props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        props.put(ProducerConfig.ACKS_CONFIG, "0"); // "all"
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000);
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, 1024000);
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 1024000);

        props.put(ProducerConfig.TIMEOUT_CONFIG, 250);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "5000");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "128");

        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    protected abstract void initialize();

    protected abstract void initializeTopic();

    protected abstract void initializeDataGenerators();

    protected abstract void initializeWorkloadData();

    protected void initializeSmallBufferProducer() {
        producer = createSmallBufferProducer();
    }

    protected void initializeLargeBufferProducer() {
        producer = createLargeBufferProducer();
    }

    protected void initializePerformanceLogWithCurrentTime() {
        Long startTime = System.nanoTime();
        performanceLog.setStartTime(startTime);
        performanceLog.setPrevTime(startTime);
    }

    protected void temporizeDataGeneration(int sleepFrequency, long step) {
        // control data generate speed
        if (sleepFrequency > 0 && step % sleepFrequency == 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    protected long getNanoTime() {
        return System.nanoTime();
    }
}