package ro.tucn.kafka_starter;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 5/3/2017.
 */
public class Broker {

    private final Logger logger = Logger.getLogger(this.getClass());

    private KafkaServerStartable kafkaServer;
    private Properties properties;
    private KafkaConfig kafkaConfig;
    private Runnable task;

    public Broker(Zookeeper zookeeper) {
        try {
            initialize();
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        kafkaServer.shutdown();
    }

    private void initialize() throws IOException {
        initializeProperties();
        initializeKafkaServerConfig();
        initializeKafkaServer();
        initializeTask();
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("server.properties"));
    }

    private void initializeKafkaServerConfig() {
        kafkaConfig = new KafkaConfig(properties);
    }

    private void initializeKafkaServer() {
        kafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    private void initializeTask() {
        task = () -> {
            kafkaServer.startup();
        };
    }

    public Runnable getTask() {
        return task;
    }

    public void start() {
        kafkaServer.shutdown();
        new Thread(task).start();
    }
}
