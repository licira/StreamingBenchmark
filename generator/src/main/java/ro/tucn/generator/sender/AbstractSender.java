package ro.tucn.generator.sender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.generator.helper.TimeHelper;

import java.io.Serializable;

/**
 * Created by Liviu on 5/9/2017.
 */
public abstract class AbstractSender<K, V> implements Serializable {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected static KafkaProducer<String, String> producer;
    protected static ProducerRecord<String, String> newRecord;

    public abstract void send(Object o);

    protected void send(String topic, K key, V value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new ProducerRecord(topic, null, timestamp, key, value);
        producer.send(newRecord);
        logger.info("Topic: " + topic
                + "\tTimestamp: " + newRecord.timestamp()
                + "\tValue: " + newRecord.value());
    }

    protected abstract String getMessageKey();

    protected abstract String getMessageValue();

    public void setProducer(KafkaProducer producer) {
        this.producer = producer;
    }
}
