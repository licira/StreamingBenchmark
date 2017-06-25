package ro.tucn.operator;

import ro.tucn.kafka.KafkaConsumerCustom;

import java.io.Serializable;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class OperatorCreator implements Serializable {

    protected String appName;

    public OperatorCreator(String appName) {
        this.appName = appName;
    }

    public abstract void Start();

    public abstract KafkaConsumerCustom getKafkaConsumerCustom();
}