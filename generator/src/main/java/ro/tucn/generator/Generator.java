package ro.tucn.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * abstract class of data ro.tucn.generator
 * Created by jun on 08/01/16.
 */
public abstract class Generator {

    private final Logger logger = Logger.getLogger(this.getClass());

    protected Properties properties;
    protected String bootstrapServersHost;
    protected String bootstrapServersPort;

    public Generator() {
        initializeProperties();
        initialzeBootstrapServersData();
    }

    public Properties getPropertiesFromResourcesFile(String configFile) {
        System.out.println(configFile);
        Properties properties = null;
        try {
            properties = new Properties();
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return properties;
    }

    public KafkaProducer<String, String> createSmallBufferProducer() {
        Properties props = getDefaultKafkaProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    public Properties getDefaultKafkaProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServersHost + ":" + bootstrapServersPort);
        props.put("group.id", "test");
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

    private void initialzeBootstrapServersData() {
        Properties properties = getPropertiesFromResourcesFile("DefaultBroker.properties");
        bootstrapServersHost = properties.getProperty("bootstrap.servers.host");
        bootstrapServersPort = properties.getProperty("bootstrap.servers.port");
    }

    private void initializeProperties() {
        properties = getPropertiesFromResourcesFile(this.getClass().getSimpleName() + ".properties");
    }

    public String getBootstrapServersHost() {
        return bootstrapServersHost;
    }

    public void setBootstrapServersHost(String bootstrapServersHost) {
        this.bootstrapServersHost = bootstrapServersHost;
    }

    public String getBootstrapServersPort() {
        return bootstrapServersPort;
    }

    public void setBootstrapServersPort(String bootstrapServersPort) {
        this.bootstrapServersPort = bootstrapServersPort;
    }
}
