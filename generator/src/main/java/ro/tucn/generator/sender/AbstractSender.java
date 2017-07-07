package ro.tucn.generator.sender;

import ro.tucn.statistics.PerformanceLog;

import java.io.Serializable;

/**
 * Created by Liviu on 6/28/2017.
 */
public abstract class AbstractSender implements Serializable {

    protected String topic;
    protected PerformanceLog performanceLog;

    public abstract void send(String topic, Object key, Object value);

    public abstract void send(Object o);

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public abstract void close();

    public void setPerformanceLog(PerformanceLog performanceLog) {
        this.performanceLog = performanceLog;
    }
}
