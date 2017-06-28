package ro.tucn.generator.sender.offline;

import org.apache.log4j.Logger;
import ro.tucn.generator.GeneratorProducer;
import ro.tucn.generator.GeneratorRecord;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractSender;

import java.io.Serializable;

/**
 * Created by Liviu on 6/28/2017.
 */
public abstract class AbstractOfflineSender extends AbstractSender implements Serializable {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private GeneratorProducer<String, String> producer;
    private GeneratorRecord<String, String> newRecord;

    public AbstractOfflineSender() {
        producer = new GeneratorProducer<>();
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void send(String topic, Object key, Object value) {
        long timestamp = TimeHelper.getNanoTime();
        newRecord = new GeneratorRecord(topic, timestamp, key, value);
        producer.send(newRecord);
        logger.info("Topic: " + topic +
                "\tMessage: " + newRecord.getValue()
        );
    }

}
