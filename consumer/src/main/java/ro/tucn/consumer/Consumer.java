package ro.tucn.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Consumer {

    private final Logger logger = Logger.getLogger(this.getClass());

    private Properties properties;
    private KafkaConsumer<String, String> consumer;

    private static String topic;
    /*
    private static final String producerHost = "54.87.160.43";
    private static final String producerPort = ":9092";
    */
    private String bootstrapServersHost;
    private String bootstrapServersPort;
    private boolean seekToBeginning;


    public Consumer(String[] args) {
        initialzeBootstrapServersData();
        properties = getConsumerProperties();
        consumer = new KafkaConsumer<String, String>(properties);
        setArgs(args);
    }

    private void initialzeBootstrapServersData() {
        Properties properties = getPropertiesFromResourcesFile("DefaultBroker.properties");
        bootstrapServersHost = properties.getProperty("bootstrap.servers.host");
        bootstrapServersPort = properties.getProperty("bootstrap.servers.port");
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

    public void run() {
        topic = "adv";
        boolean assign = false;
        if(assign) {
            TopicPartition tp = new TopicPartition(topic, 0);
            List<TopicPartition> tps = Arrays.asList(tp);
            consumer.assign(tps);
            if(seekToBeginning) {
                consumer.seekToBeginning(tps);
            }
        } else {
            consumer.subscribe(Arrays.asList(topic));
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.println(record.offset() + " " + record.key() + " " + record.value());
        }
    }

    public Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServersHost + ":" + bootstrapServersPort);
        props.put("group.id", "test");
        //props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void setSeekToBeginning(boolean seekToBeginning) {
        this.seekToBeginning = seekToBeginning;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setArgs(String[] args) {
        if(args.length > 0) {
            try {
                if (!args[0].isEmpty()) {
                    setTopic(args[0]);
                }
                if (args[1].equals("from-beggining")) {
                    setSeekToBeginning(true);
                }
            } catch (NullPointerException e) {

            } catch (ArrayIndexOutOfBoundsException e) {

            }
        }
    }
}
