package ro.tucn.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Liviu on 6/28/2017.
 */
public class GeneratorProducer<K, V> {

    private Map<String, List<Map<K, V>>> buffer = new HashMap<>();

    public void send(GeneratorRecord<K, V> record) {
        String topic = record.getTopic();
        List<Map<K, V>> kvList = buffer.get(topic);
        buffer.remove(topic);
        if (kvList == null) {
            kvList = new ArrayList<>();
        }
        Map<K, V> kvMap = new HashMap<>();
        kvMap.put(record.getKey(), record.getValue());
        kvList.add(kvMap);
        buffer.put(topic, kvList);
    }

    public List<Map<K, V>> read(String topic) {
        List<Map<K, V>> list = buffer.get(topic);
        //buffer.remove(topic);
        return list;
    }

    public void close() {
        buffer.clear();
    }
}
