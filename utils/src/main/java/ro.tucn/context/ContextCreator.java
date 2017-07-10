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

    public ContextCreator(String appName) {
        this.appName = appName;
    }

    public abstract void start();

    public abstract AbstractKafkaConsumerCustom getKafkaConsumerCustom();

    public abstract AbstractGeneratorConsumer getGeneratorConsumer();

    public abstract Object getPerformanceListener();

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}