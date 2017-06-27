package ro.tucn.generator.sender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.generator.creator.ProducerCreator;
import ro.tucn.generator.helper.TimeHelper;

import java.io.Serializable;

/**
 * Created by Liviu on 5/9/2017.
 */
public abstract class AbstractMessageSender implements Serializable {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    protected static KafkaProducer<String, String> producer;
    protected static ProducerRecord<String, String> newRecord;
    protected String topic;
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

    public void send(String topic, Object key, Object value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new ProducerRecord(topic, 0, timestamp, key, value);
        producer.send(newRecord);
        logger.info("Topic: " + topic +
                "\tPartition:" + newRecord.partition() +
                "\tMessage: " + newRecord.value()
        );
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
