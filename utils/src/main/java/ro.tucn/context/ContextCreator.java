package ro.tucn.context;

import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;

import java.io.Serializable;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class ContextCreator implements Serializable {

    protected String appName;

    public ContextCreator(String appName) {
        this.appName = appName;
    }

    public abstract void Start();

    public abstract AbstractKafkaConsumerCustom getKafkaConsumerCustom();

    public abstract AbstractGeneratorConsumer getGeneratorConsumer();
}