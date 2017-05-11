package ro.tucn.generator.sender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.producer.ProducerCreator;

import java.io.Serializable;

/**
 * Created by Liviu on 5/9/2017.
 */
public abstract class AbstractMessageSender implements Serializable {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected static KafkaProducer<String, String> producer;
    protected static ProducerRecord<String, String> newRecord;
    private ProducerCreator producerCreator;

    public AbstractMessageSender() {
        producerCreator = new ProducerCreator();
    }

    public abstract void send(Object o);

    public void initializeSmallBufferProducer(String bootstrapServers) {
        producer = producerCreator.createSmallBufferProducer(bootstrapServers);
    }

    public void close() {
        producer.close();
    }

    protected abstract String getMessageKey();

    protected abstract String getMessageValue();

    protected void send(String topic, Object key, Object value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new ProducerRecord(topic, null, timestamp, key, value);
        producer.send(newRecord);
        logger.info("Topic: " + topic
                + "\tTimestamp: " + newRecord.timestamp()
                + "\tKey: " + newRecord.key()
                + "\tValue: " + newRecord.value());
    }
}
