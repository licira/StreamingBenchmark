package ro.tucn.generator;

import static ro.tucn.exceptions.ExceptionMessage.INVALID_TIMESTAMP_MSG;
import static ro.tucn.exceptions.ExceptionMessage.TOPIC_CANNOT_BE_NULL_MSG;

/**
 * Created by Liviu on 6/28/2017.
 */
public class GeneratorRecord<K, V> {

    private final String topic;
    private final long timestamp;
    private final K key;
    private final V value;

    public GeneratorRecord(String topic, Long timestamp, K key, V value) {
        if (topic == null) {
            throw new IllegalArgumentException(TOPIC_CANNOT_BE_NULL_MSG);
        } else if (timestamp != null && timestamp < 0L) {
            throw new IllegalArgumentException(String.format(INVALID_TIMESTAMP_MSG, new Object[]{timestamp}));
        }
        this.topic = topic;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
