package ro.tucn.consumer;

import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class AbstractConsumer {

    protected int parallelism;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    protected String getTopicFromProperties(Properties properties, String topicPropertyName) {
        return properties.getProperty(topicPropertyName);
    }
}
