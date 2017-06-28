package ro.tucn.generator.sender;

/**
 * Created by Liviu on 6/28/2017.
 */
public abstract class AbstractSender {

    protected String topic;

    public abstract void send(String topic, Object key, Object value);

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
