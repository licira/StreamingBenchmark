package ro.tucn.generator.sender;

import java.io.Serializable;

/**
 * Created by Liviu on 6/28/2017.
 */
public abstract class AbstractSender implements Serializable {

    protected String topic;

    public abstract void send(String topic, Object key, Object value);

    public abstract void send(Object o);

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
