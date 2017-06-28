package ro.tucn.generator;

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
            throw new IllegalArgumentException("Topic cannot be null.");
        } else if (timestamp != null && timestamp < 0L) {
            throw new IllegalArgumentException(String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", new Object[]{timestamp}));
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
