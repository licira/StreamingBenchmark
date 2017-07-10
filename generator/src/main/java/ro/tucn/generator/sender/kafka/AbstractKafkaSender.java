package ro.tucn.generator.sender.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.generator.creator.ProducerCreator;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractSender;

/**
 * Created by Liviu on 5/9/2017.
 */
public abstract class AbstractKafkaSender extends AbstractSender  {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected KafkaProducer<String, String> producer;
    protected ProducerRecord<String, String> newRecord;
    private ProducerCreator producerCreator;

    public AbstractKafkaSender() {
        producerCreator = new ProducerCreator();
    }

    public void initializeSmallBufferProducer(String bootstrapServers) {
        producer = producerCreator.createSmallBufferProducer(bootstrapServers);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void send(String topic, Object key, Object value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new ProducerRecord(topic, 0, timestamp, key, value);
        producer.partitionsFor(topic);
        producer.send(newRecord);
        //performanceLog.logSize(key);
        //performanceLog.logSize(value);
        //performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
        logger.info("Topic: " + topic +
                "\tPartition: " + newRecord.partition() +
                "\tMessage: " + newRecord.value()
        );
    }
}
