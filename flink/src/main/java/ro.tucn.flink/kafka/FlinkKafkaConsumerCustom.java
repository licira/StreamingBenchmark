package ro.tucn.flink.kafka;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import ro.tucn.flink.operator.FlinkOperator;
import ro.tucn.flink.operator.stream.FlinkStreamPairOperator;
import ro.tucn.kMeans.Point;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.BatchOperator;
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
public class FlinkKafkaConsumerCustom extends KafkaConsumerCustom {

    private StreamExecutionEnvironment env;

    public FlinkKafkaConsumerCustom(StreamExecutionEnvironment env) {
        super();
        this.env = env;
    }

    @Override
    public StreamOperator<String> getStringOperator(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        DataStream<String> stream = getStringStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return new FlinkOperator<>(stream, parallelism);
    }

    @Override
    public StreamPairOperator<String, String> getStreamPairOperator(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        DataStream<Tuple2<String, String>> pairStream = getPairStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return new FlinkStreamPairOperator<>(pairStream, parallelism);
    }

    @Override
    public StreamOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        DataStream<String> stream = getStreamFromKafka(properties, topicPropertyName);
        DataStream<TimeHolder<String>> TimeHolderDataStream = stream.map((MapFunction<String, TimeHolder<String>>) value -> {
            String[] list = value.split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new TimeHolder<>(list[0], Long.parseLong(list[1]));
            }
            return new TimeHolder<>(value, System.currentTimeMillis());
        });
        return new FlinkOperator<>(TimeHolderDataStream, parallelism);
    }

    @Override
    public StreamOperator<Point> getPointOperator(Properties properties, String topicPropertyName) {
        setEnvParallelism(parallelism);
        DataStream<String> jsonStream = getStringSstreamFromKafka(properties, topicPropertyName);
        DataStream<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new FlinkOperator<>(pointStream, parallelism);
    }

    @Override
    public BatchOperator<String> getBatchStringOperator(Properties properties, String topicPropertyName) {
        return null;
    }

    private DataStream<String> getStringSstreamFromKafka(Properties properties, String topicPropertyName) {
        DataStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
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
}
