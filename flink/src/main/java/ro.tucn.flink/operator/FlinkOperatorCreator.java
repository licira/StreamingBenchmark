package ro.tucn.flink.operator;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;
import ro.tucn.flink.kafka.FlinkKafkaConsumerCustom;
import ro.tucn.kMeans.Point;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.Constants;
import ro.tucn.util.Message;
import ro.tucn.util.WithTime;
import scala.Tuple2;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkOperatorCreator extends OperatorCreator {

    private static final Logger logger = Logger.getLogger(FlinkOperatorCreator.class);

    private final StreamExecutionEnvironment env;
    private Properties properties;

    public FlinkOperatorCreator(String name) throws IOException {
        super(name);
        System.out.println("1");
        initializeProperties();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("2");
    }

    @Override
    public void Start() {
        try {
            env.execute(appName);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public KafkaConsumerCustom getKafkaConsumerCustom() {
        return new FlinkKafkaConsumerCustom(env);
    }

    @Override
    public Operator<String> getStringStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        setEnvParallelism(parallelism);
        DataStream<String> stream = getStringStreamFromKafka(properties, topicPropertyName);
        return new FlinkOperator<>(stream, parallelism);
    }

    @Override
    public PairOperator<String, String> getPairStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        setEnvParallelism(parallelism);
        DataStream<Tuple2<String, String>> pairStream = getPairStreamFromKafka(properties, topicPropertyName);
        return new FlinkPairOperator<>(pairStream, parallelism);
    }

    @Override
    public Operator<WithTime<String>> getStringStreamWithTimeFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        setEnvParallelism(parallelism);
        DataStream<String> stream = getStreamFromKafka(properties, topicPropertyName);
        DataStream<WithTime<String>> withTimeDataStream = stream.map((MapFunction<String, WithTime<String>>) value -> {
            String[] list = value.split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new WithTime<>(list[0], Long.parseLong(list[1]));
            }
            return new WithTime<>(value, System.currentTimeMillis());
        });
        return new FlinkOperator<>(withTimeDataStream, parallelism);
    }

    @Override
    public Operator<Point> getPointStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        setEnvParallelism(parallelism);
        DataStream<String> jsonStream = getStringStreamFromKafka(properties, topicPropertyName);
        DataStream<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new FlinkOperator<>(pointStream, parallelism);
    }

    private DataStream<Point> getPointStreamFromJsonStream(DataStream<String> jsonStream) {
        DataStream<Point> pointStream = jsonStream.map(new MapFunction<String, Point>() {
            @Override
            public Point map(String s) throws Exception {
                Gson gson = new Gson();
                Point point = gson.fromJson(s, Point.class);
                return point;
            }
        });
        return pointStream;
    }

    private DataStream<String> getStringStreamFromKafka(Properties properties, String topicPropertyName) {
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        DataStream<String> stringStreamWithJsonAsValue = getStringStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return stringStreamWithJsonAsValue;
    }

    private DataStream<Tuple2<String,String>> getPairStreamFromKafka(Properties properties, String topicPropertyName) {
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        DataStream<Tuple2<String, String>> pairStreamWithJsonAsValue = getPairStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return pairStreamWithJsonAsValue;
    }

    private DataStream<String> getStringStreamFromStreamWithJsonAsValue(DataStream<String> streamWithJsonAsValue) {
        DataStream<String> stream = streamWithJsonAsValue.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                Gson gson = new Gson();
                Message msg = gson.fromJson(s, Message.class);
                return msg.getValue();
            }
        });
        return stream;
    }

    private DataStream<Tuple2<String,String>> getPairStreamFromStreamWithJsonAsValue(DataStream<String> streamWithJsonAsValue) {
        DataStream<Tuple2<String, String>> pairStream = streamWithJsonAsValue.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                Gson gson = new Gson();
                Message msg = gson.fromJson(s, Message.class);
                return new Tuple2<String, String>(msg.getKey(), msg.getValue());
            }
        });
        return pairStream;
    }

    private DataStream<String> getStringWithJsonAsValueStreamFromKafka(Properties properties, String topicPropertyName) {
        DataStream<String> stream = getStreamFromKafka(properties, topicPropertyName);
        return stream;
    }

    private DataStream<String> getStreamFromKafka(Properties properties, String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        properties.setProperty("flink.starting-position", "largest");
        FlinkKafkaConsumer081<String> kafkaConsumer = new FlinkKafkaConsumer081<>(topic, new SimpleStringSchema(), properties);
        return env.addSource(kafkaConsumer);
    }

    private String getTopicFromProperties(Properties properties, String topicPropertyName) {
        return properties.getProperty(topicPropertyName);
    }

    private void setEnvParallelism(int parallelism) {
        env.setParallelism(parallelism);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("flink-cluster.properties"));
    }
}
