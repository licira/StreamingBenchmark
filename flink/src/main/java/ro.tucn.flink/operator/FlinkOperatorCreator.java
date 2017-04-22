package ro.tucn.flink.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.WorkloadOperator;
import ro.tucn.util.Constants;
import ro.tucn.util.WithTime;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkOperatorCreator extends OperatorCreator {

    private final StreamExecutionEnvironment env;
    private Properties properties;

    public FlinkOperatorCreator(String name) throws IOException {
        super(name);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        initializeProperties();
    }

    @Override
    public WorkloadOperator<String> stringStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        String topic = properties.getProperty(topicPropertyName);
        env.setParallelism(parallelism);
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer082<>(topic, new SimpleStringSchema(), properties));
        return new FlinkWorkloadOperator<>(stream, parallelism);
    }

    @Override
    public WorkloadOperator<WithTime<String>> stringStreamFromKafkaWithTime(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        String topic = properties.getProperty(topicPropertyName);
        env.setParallelism(parallelism);
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer082<>(topic, new SimpleStringSchema(), properties));
        DataStream<WithTime<String>> withTimeDataStream = stream.map((MapFunction<String, WithTime<String>>) value -> {
            String[] list = value.split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new WithTime<>(list[0], Long.parseLong(list[1]));
            }
            return new WithTime<>(value, System.currentTimeMillis());
        });
        return new FlinkWorkloadOperator<>(withTimeDataStream, parallelism);
    }

    @Override
    public WorkloadOperator<Point> pointStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        String topic = properties.getProperty(topicPropertyName);
        env.setParallelism(parallelism);
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer082<>(topic, new SimpleStringSchema(), properties));
        DataStream<Point> pointStream = stream.map((MapFunction<String, Point>) value -> {
            String[] list = value.split(Constants.TimeSeparatorRegex);
            long time = System.currentTimeMillis();
            if (list.length == 2) {
                time = Long.parseLong(list[1]);
            }
            String[] strs = list[0].split("\t");
            double[] position = new double[strs.length];
            for (int i = 0; i < strs.length; i++) {
                position[i] = Double.parseDouble(strs[i]);
            }
            return new Point(position, time);
        });
        return new FlinkWorkloadOperator<>(pointStream, parallelism);
    }

    @Override
    public void Start() {
        try {
            env.execute(appName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("flink-cluster.properties"));
    }
}
