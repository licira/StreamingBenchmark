package ro.tucn.generator.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by Liviu on 5/6/2017.
 */
public class ProducerCreator {

    public KafkaProducer<String, String> createSmallBufferProducer(String bootstrapServers) {
        Properties props = getDefaultKafkaProducerProperties(bootstrapServers);
        return createKafkaProducerWithProperties(props);
    }

    public KafkaProducer<String, String> createLargeBufferProducer(String bootstrapServers) {
        Properties props = getLargeBufferKafkaProducerProperties(bootstrapServers);
        return createKafkaProducerWithProperties(props);
    }

    private KafkaProducer<String, String> createKafkaProducerWithProperties(Properties props) {
        return new KafkaProducer<String, String>(props);
    }

    private Properties getDefaultKafkaProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.enable", "false");
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

    private Properties getLargeBufferKafkaProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
}
