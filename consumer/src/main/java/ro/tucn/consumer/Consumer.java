package ro.tucn.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import ro.tucn.util.ConfigReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Liviu on 4/5/2017.
 */
public class Consumer {

    private static final Logger logger = Logger.getLogger(Consumer.class.getSimpleName());
    private static String topic;
    private ConfigReader configReader = new ConfigReader();
    private Properties properties;
    private KafkaConsumer<String, String> consumer;
    private String bootstrapServerHost;
    private boolean seekToBeginning;
    private boolean assign;

    public Consumer(String[] args) {
        initialzeBootstrapServersData();
        properties = getDefaultConsumerProperties();
        consumer = new KafkaConsumer<String, String>(properties);
        setArgs(args);
    }

    public void run() {
        logger.info(topic);
        if (assign) {
            TopicPartition tp = new TopicPartition(topic, 0);
            List<TopicPartition> tps = Arrays.asList(tp);
            consumer.assign(tps);
            //if(seekToBeginning)
            {
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

    public Properties getDefaultConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServerHost);
        props.put("group.id", "test");
        //props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "false");
        //props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void setArgs(String[] args) {
        if (args.length > 0) {
            try {
                if (!args[0].isEmpty()) {
                    topic = args[0];
                } else {
                    topic = "";
                }
                if (!args[1].isEmpty() && args[1].equals("assign")) {
                    assign = true;
                }
            } catch (NullPointerException e) {

            } catch (ArrayIndexOutOfBoundsException e) {

            }
        }
    }

    private void initialzeBootstrapServersData() {
        Properties properties = null;
        try {
            properties = configReader.getPropertiesFromResourcesFile("DefaultBroker.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        bootstrapServerHost = properties.getProperty("bootstrap.servers.host");
    }
}
