package ro.tucn.spark.consumer;

import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.spark.operator.SparkStreamOperator;
import ro.tucn.spark.operator.stream.SparkStreamPairOperator;
import ro.tucn.util.Constants;
import ro.tucn.util.Message;
import ro.tucn.util.TimeHolder;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

/**
 * Created by Liviu on 6/24/2017.
 */
public class SparkKafkaConsumerCustom extends AbstractKafkaConsumerCustom {

    private static final Logger logger = Logger.getLogger(SparkKafkaConsumerCustom.class);

    private JavaSparkContext sc;
    private JavaStreamingContext jssc;

    public SparkKafkaConsumerCustom(JavaStreamingContext jssc, JavaSparkContext sc) {
        super();
        this.jssc = jssc;
        this.sc = sc;
    }

    @Override
    public StreamOperator<String> getStringOperator(Properties properties, String topicPropertyName) {
        logger.info("11");
        JavaDStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaDStream<String> stream = getStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        //stream.print();
        return new SparkStreamOperator<String>(stream, parallelism);
    }

    @Override
    public StreamPairOperator<String, String> getStreamPairOperator(Properties properties, String topicPropertyName) {
        JavaDStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaPairDStream<String, String> pairStream = getPairStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        //stream.print();
        return new SparkStreamPairOperator(pairStream, parallelism);
    }

    @Override
    public StreamOperator<Point> getPointOperator(Properties properties, String topicPropertyName) {
        JavaDStream<String> jsonStream = getStringStreamFromKafka(properties, topicPropertyName);
        //jsonStream.print();
        JavaDStream<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new SparkStreamOperator<Point>(pointStream, parallelism);
    }

    @Override
    public SparkStreamOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties, String topicPropertyName) {
        JavaPairDStream<String, String> pairStream = getPairStreamFromKafka(properties, topicPropertyName);
        JavaDStream<TimeHolder<String>> stream = pairStream.map(stringStringTuple2 -> {
            String[] list = stringStringTuple2._2().split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new TimeHolder<String>(list[0], Long.parseLong(list[1]));
            }
            return new TimeHolder(stringStringTuple2._2(), System.nanoTime());
        });
        return new SparkStreamOperator<TimeHolder<String>>(stream, parallelism);
    }

    private JavaDStream<String> getStringStreamFromKafka(Properties properties, String topicPropertyName) {
        JavaDStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaDStream<String> stream = getStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return stream;
    }

    private JavaPairDStream<String, String> getPairStreamFromKafka(Properties properties, String topicPropertyName) {
        JavaDStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaPairDStream<String, String> pairStream = getPairStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        return pairStream;
    }

    private JavaDStream<String> getStreamFromStreamWithJsonAsValue(JavaDStream<String> streamWithJsonAsValue) {
        JavaDStream<String> stream = streamWithJsonAsValue.map(s -> {
            Gson gson = new Gson();
            Message msg = gson.fromJson(s, Message.class);
            return msg.getValue();
        });
        return stream;
    }

    private JavaPairDStream<String, String> getPairStreamFromStreamWithJsonAsValue(JavaDStream<String> streamWithJsonAsValue) {
        JavaPairDStream<String, String> pairStream = streamWithJsonAsValue.mapToPair(s -> {
            Gson gson = new Gson();
            Message msg = gson.fromJson(s, Message.class);
            return new Tuple2<String, String>(msg.getKey(), msg.getValue());
        });
        return pairStream;
    }

    private JavaDStream<Point> getPointStreamFromJsonStream(JavaDStream<String> jsonStream) {
        JavaDStream<Point> pointStream = jsonStream.map(s -> {
            Gson gson = new Gson();
            Point point = gson.fromJson(s, Point.class);
            return point;
        });
        return pointStream;
    }

    private JavaDStream<String> getStringWithJsonAsValueStreamFromKafka(Properties properties, String topicPropertyName) {
        logger.info("111");
        JavaPairDStream<String, String> pairStreamWithJsonAsValue = getDirectStreamFromKafka(properties, topicPropertyName);
        JavaDStream<String> streamWithJsonAsValue = pairStreamWithJsonAsValue.map(jsonTuple -> jsonTuple._2());
        return streamWithJsonAsValue;
    }

    private JavaPairDStream<String, String> getDirectStreamFromKafka(Properties properties, String topicPropertyName) {
        HashSet<String> topicsSet = getTopicSetFromProperites(topicPropertyName, properties);
        HashMap<String, String> kafkaParams = getKafkaParamsFromProperties(properties);
        return KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
    }

    private HashSet<String> getTopicSetFromProperites(String topicPropertyName, Properties properties) {
        String topics = properties.getProperty(topicPropertyName);
        String[] split = splitTopics(topics);
        return new HashSet(Arrays.asList(split));
    }

    private HashMap getKafkaParamsFromProperties(Properties properties) {
        HashMap<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", (String) properties.get("bootstrap.servers"));
        kafkaParams.put("auto.offset.reset", (String) properties.get("auto.offset.reset"));
        kafkaParams.put("zookeeper.connect", (String) properties.get("zookeeper.connect"));
        kafkaParams.put("group.id", (String) properties.get("group.id"));
        kafkaParams.put("auto.create.topics", "true");
        return kafkaParams;
    }

    private String[] splitTopics(String topics) {
        return topics.split(",");
    }
}
