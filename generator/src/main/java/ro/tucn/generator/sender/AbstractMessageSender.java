package ro.tucn.generator.sender;

import com.google.gson.Gson;
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

    protected static KafkaProducer<String, String> producer;
    protected static ProducerRecord<String, String> newRecord;
    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    protected String topic;
    private ProducerCreator producerCreator;
    private Gson gson;

    public AbstractMessageSender() {
        producerCreator = new ProducerCreator();
        gson = new Gson();
    }

    public abstract void send(Object o);

    public void initializeSmallBufferProducer(String bootstrapServers) {
        producer = producerCreator.createSmallBufferProducer(bootstrapServers);
    }

    public void close() {
        producer.close();
    }

    protected String toJson(Object o) {
        return gson.toJson(o);
    }

    public void send(String topic, Object key, Object value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new ProducerRecord(topic, null, timestamp, key, value);
        //producer.send(newRecord);
        logger.info("Topic: " + topic + "\tMessage: " + newRecord.value());
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
