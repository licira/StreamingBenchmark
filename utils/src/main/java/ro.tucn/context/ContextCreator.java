package ro.tucn.context;

import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;

import java.io.Serializable;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class ContextCreator implements Serializable {

    protected int parallelism = -1;

    protected String appName;
    protected String frameworkName;
    protected String dataMode;

    public ContextCreator(String appName) {
        this.appName = appName;
    }

    public abstract void start();

    public abstract AbstractKafkaConsumerCustom getKafkaConsumerCustom();

    public abstract AbstractGeneratorConsumer getGeneratorConsumer();

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getFrameworkName() {
        return frameworkName;
    }

    public String getDataMode() {
        return dataMode;
    }
}