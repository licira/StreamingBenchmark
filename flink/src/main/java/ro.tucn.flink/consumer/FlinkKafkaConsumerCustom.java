package ro.tucn.flink.consumer;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.flink.operator.FlinkStreamOperator;
import ro.tucn.flink.operator.stream.FlinkStreamPairOperator;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.util.Constants;
import ro.tucn.util.Message;
import ro.tucn.util.TimeHolder;
import scala.Tuple2;

import java.util.Properties;

/**
 * Created by Liviu on 6/24/2017.
 */
public class FlinkKafkaConsumerCustom extends AbstractKafkaConsumerCustom {

    private StreamExecutionEnvironment env;

    public FlinkKafkaConsumerCustom(StreamExecutionEnvironment env) {
        super();
        this.env = env;
    }

    @Override
    public StreamOperator<String> getStringOperator(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        String topic = getTopicFromProperties(properties, topicPropertyName);
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topic);
        DataStream<String> stream = getStringStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return new FlinkStreamOperator<>(stream, parallelism);
    }

    @Override
    public StreamPairOperator<String, String> getStreamPairOperator(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        String topic = getTopicFromProperties(properties, topicPropertyName);
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topic);
        DataStream<Tuple2<String, String>> pairStream = getPairStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return new FlinkStreamPairOperator<>(pairStream, parallelism);
    }

    @Override
    public StreamOperator<Point> getPointOperator(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        String topic = getTopicFromProperties(properties, topicPropertyName);
        DataStream<String> jsonStream = getStringStreamFromKafka(properties, topic);
        DataStream<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new FlinkStreamOperator<>(pointStream, parallelism);
    }

    @Override
    public StreamOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties, String topic) {
        setEnvParallelism(parallelism);
        DataStream<String> stream = getStreamFromKafka(properties, topic);
        DataStream<TimeHolder<String>> TimeHolderDataStream = stream.map((MapFunction<String, TimeHolder<String>>) value -> {
            String[] list = value.split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new TimeHolder<>(list[0], Long.parseLong(list[1]));
            }
            return new TimeHolder<>(value, System.currentTimeMillis());
        });
        return new FlinkStreamOperator<>(TimeHolderDataStream, parallelism);
    }

    private DataStream<String> getStringStreamFromKafka(Properties properties, String topic) {
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topic);
        DataStream<String> stream = getStringStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return stream;
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

    private DataStream<Tuple2<String, String>> getPairStreamFromStreamWithJsonAsValue(DataStream<String> streamWithJsonAsValue) {
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

    private DataStream<String> getStringWithJsonAsValueStreamFromKafka(Properties properties, String topic) {
        DataStream<String> stream = getStreamFromKafka(properties, topic);
        return stream;
    }

    private DataStream<String> getStreamFromKafka(Properties properties, String topic) {
        properties.setProperty("flink.starting-position", "largest");
        FlinkKafkaConsumer081<String> kafkaConsumer = new FlinkKafkaConsumer081<>(topic, new SimpleStringSchema(), properties);
        return env.addSource(kafkaConsumer);
    }

    private void setEnvParallelism(int parallelism) {
        env.setParallelism(parallelism);
    }
}
