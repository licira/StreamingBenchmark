package ro.tucn.spark.operator;

import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.spark.statistics.PerformanceStreamingListener;
import ro.tucn.util.Constants;
import ro.tucn.util.Message;
import ro.tucn.util.WithTime;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkOperatorCreator extends OperatorCreator {

    private static final Logger logger = Logger.getLogger(SparkOperatorCreator.class);

    private static final String TOPIC_SPLITTER = ",";

    public static JavaStreamingContext jssc;
    public static JavaSparkContext sc;
    private Properties properties;

    public SparkOperatorCreator(String appName) throws IOException {
        super(appName);
        initializeProperties();
        initializeJavaStreamingContext(appName);
    }

    @Override
    public void Start() {
        jssc.addStreamingListener(new PerformanceStreamingListener());
        jssc.checkpoint("/tmp/spark/checkpoint");
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Operator<String> getStringStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        JavaDStream<String> stream = getStringStreamFromKafka(properties, topicPropertyName);
        return new SparkOperator<String>(stream, parallelism);
    }

    @Override
    public PairOperator<String, String> getPairStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        JavaPairDStream<String, String> pairStream = getPairStreamFromKafka(properties, topicPropertyName);
        pairStream.print();
        return new SparkPairOperator(pairStream, parallelism);
    }

    @Override
    public Operator<Point> getPointStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        JavaDStream<String> jsonStream = getStringStreamFromKafka(properties, topicPropertyName);
        //jsonStream.print();
        JavaDStream<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new SparkOperator<Point>(pointStream, parallelism);
    }

    @Override
    public SparkOperator<WithTime<String>> getStringStreamWithTimeFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        JavaPairDStream<String, String> pairStream = getPairStreamFromKafka(properties, topicPropertyName);
        JavaDStream<WithTime<String>> stream = pairStream.map(stringStringTuple2 -> {
            String[] list = stringStringTuple2._2().split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new WithTime<String>(list[0], Long.parseLong(list[1]));
            }
            return new WithTime(stringStringTuple2._2(), System.nanoTime());
        });
        return new SparkOperator<WithTime<String>>(stream, parallelism);
    }

    private JavaDStream<String> getStringStreamFromKafka(Properties properties, String topicPropertyName) {
        logger.info("11");
        JavaDStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaDStream<String> stream = getStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        //stream.print();
        return stream;
    }

    private JavaPairDStream<String, String> getPairStreamFromKafka(Properties properties, String topicPropertyName) {
        JavaDStream<String> streamWithJsonAsValue = getStringWithJsonAsValueStreamFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaPairDStream<String, String> stream = getPairStreamFromStreamWithJsonAsValue(streamWithJsonAsValue);
        //stream.print();
        return stream;
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
        return kafkaParams;
    }

    private String[] splitTopics(String topics) {
        return topics.split(TOPIC_SPLITTER);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("spark-cluster.properties"));
    }

    private void initializeJavaStreamingContext(String appName) {
        SparkConf conf = new SparkConf()
                .setMaster(getMaster())
                .setAppName(appName)
                .set("spark.driver.memory", "256m")
                .set("spark.executor.memory", "768m")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                //.set("num-executors", "1")
                //.set("executor.instances", "1")
                //.set("cores.max", "1")
                //.set("spark.streaming.ui.retainedBatches", "2000")
                ;
        sc = new JavaSparkContext(conf);
        jssc = new JavaStreamingContext(sc, Durations.milliseconds(this.getDurationsMilliseconds()));
    }

    private String getMaster() {
        return properties.getProperty("cluster.master");
    }

    private long getDurationsMilliseconds() {
        return Long.parseLong(properties.getProperty("streaming.durations.milliseconds"));
    }
}