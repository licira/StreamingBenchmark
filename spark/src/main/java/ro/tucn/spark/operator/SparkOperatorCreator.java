package ro.tucn.spark.operator;

import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import ro.tucn.spark.statistics.PerformanceStreamingListener;
import ro.tucn.util.Constants;
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

    private static Function<Tuple2<String, String>, String> mapFunction
            = (Function<Tuple2<String, String>, String>) stringStringTuple2 -> stringStringTuple2._2();
    private static Function<Tuple2<String, String>, WithTime<String>> mapFunctionWithTime
            = (Function<Tuple2<String, String>, WithTime<String>>) stringStringTuple2 -> {
        String[] list = stringStringTuple2._2().split(Constants.TimeSeparatorRegex);
        if (list.length == 2) {
            return new WithTime<String>(list[0], Long.parseLong(list[1]));
        }
        return new WithTime(stringStringTuple2._2(), System.nanoTime());
    };
    private static Function<Tuple2<String, String>, Point> mapToPointFunction
            = (Function<Tuple2<String, String>, Point>) stringStringTuple2 -> {
                String value = stringStringTuple2._2();
                String[] locationsString = value.split(" ");
                Point point = new Point();
                //point.setTime(Long.parseLong(stringStringTuple2._1()));
                Double[] locations = new Double[locationsString.length];
                int idx = 0;
                for (String location : locationsString) {
                    locations[idx] = Double.parseDouble(location);
                    idx++;
                }
                return point;
            };

    public JavaStreamingContext jssc;
    private Properties properties;

    public SparkOperatorCreator(String appName) throws IOException {
        super(appName);
        initializeProperties();
        initializeJavaStreamingContext(appName);
    }

    @Override
    public void Start() {
        jssc.addStreamingListener(new PerformanceStreamingListener());
        //jssc.checkpoint("/tmp/log-analyzer-streaming");
        jssc.checkpoint("/tmp/spark/checkpoint");
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Operator<String> getStringStreamFromKafka(Properties properties,
                                                     String topicPropertyName,
                                                     String componentId,
                                                     int parallelism) {
        String topic = properties.getProperty(topicPropertyName);
        HashSet<String> topicsSet = new HashSet(Arrays.asList(topic.split(",")));

        HashMap<String, String> kafkaParams = createKafkaParams(properties);
        // Create direct kafka stream with brokers and topics
        JavaPairDStream<String, String> messages = createDirectStream(kafkaParams, topicsSet);
        //logger.info(topic);
        messages.print();
        JavaDStream<String> lines = messages.map(mapFunction);
        lines.print();
        return new SparkOperator(lines, parallelism);
    }

    @Override
    public PairOperator<String, String> getPairStreamFromKafka(Properties properties, String topicPropertyName, String componentId, int parallelism) {
        String topic = properties.getProperty(topicPropertyName);
        HashSet<String> topicsSet = new HashSet(Arrays.asList(topic.split(",")));
        HashMap<String, String> kafkaParams = createKafkaParams(properties);
        JavaPairDStream<String, String> messages = createDirectStream(kafkaParams, topicsSet);
        return new SparkPairOperator(messages, parallelism);
    }

    @Override
    public SparkOperator<WithTime<String>> getStringStreamWithTimeFromKafka(Properties properties,
                                                                            String topicPropertyName,
                                                                            String componentId,
                                                                            int parallelism) {
        String topic = properties.getProperty(topicPropertyName);
        HashSet<String> topicsSet = new HashSet(Arrays.asList(topic));

        HashMap<String, String> kafkaParams = createKafkaParams(properties);
        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = (JavaPairInputDStream<String, String>) createDirectStream(kafkaParams, topicsSet);
        print(messages);
        JavaDStream<WithTime<String>> lines = messages.map(mapFunctionWithTime);
        return new SparkOperator(lines, parallelism);
    }

    @Override
    public Operator<Point> getPointStreamFromKafka(Properties properties,
                                                   String topicPropertyName,
                                                   String componentId,
                                                   int parallelism) {
        /*NOT OK*/
        String topic = properties.getProperty(topicPropertyName);
        HashSet<String> topicsSet = new HashSet(Arrays.asList(topic));

        HashMap<String, String> kafkaParams = createKafkaParams(properties);
        // Create direct kafka stream with brokers and topics
        JavaPairDStream<String, String> messages = createDirectStream(kafkaParams, topicsSet);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + topic);
        print(messages);
        JavaDStream<Point> lines = messages.map(mapToPointFunction);
        printPoints(lines);
        return new SparkOperator(lines, parallelism);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("spark-cluster.properties"));
    }

    private void initializeJavaStreamingContext(String appName) {
        SparkConf conf = new SparkConf().setMaster(this.getMaster()).setAppName(appName);
        conf.set("spark.streaming.ui.retainedBatches", "2000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        jssc = new JavaStreamingContext(sc, Durations.milliseconds(this.getDurationsMilliseconds()));
    }

    private HashMap createKafkaParams(Properties properties) {
        HashMap<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", (String) properties.get("bootstrap.servers"));
        kafkaParams.put("auto.offset.reset", (String) properties.get("auto.offset.reset"));
        kafkaParams.put("zookeeper.connect", (String) properties.get("zookeeper.connect"));
        kafkaParams.put("group.id", (String) properties.get("group.id"));
        return kafkaParams;
    }

    private JavaPairDStream<String, String> createDirectStream(HashMap<String, String> kafkaParams, HashSet<String> topicsSet) {
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

    private void print(JavaPairDStream<String, String> messages) {
        VoidFunction2<JavaPairRDD<String, String>, Time> function = (VoidFunction2<JavaPairRDD<String, String>, Time>) (rdd, time) -> {
            logger.info("===================================");
            logger.info("New Events for " + time + " batch:");
            for (Tuple2<String, String> tuple : rdd.collect()) {
                logger.info("Tuples: " + tuple._1 + ": " + tuple._2);
            }
            logger.info("===================================");
        };
        //messages.foreachRDD(function);
        messages.print();
    }

    private void print(JavaDStream<String> lines) {
        VoidFunction2<JavaRDD<String>, Time> voidFunction = (VoidFunction2<JavaRDD<String>, Time>) (rdd, time) -> {
            logger.info("===================================");
            logger.info(" Number of records in this batch: " + rdd.count());
            for (String value : rdd.collect()) {
                logger.info("Values: " + value);
            }
            logger.info("===================================");
        };
        //lines.foreachRDD(voidFunction);
        lines.print();
    }

    private void printPoints(JavaDStream<Point> lines) {
        VoidFunction2<JavaRDD<Point>, Time> voidFunction2 = (VoidFunction2<JavaRDD<Point>, Time>) (rdd, time) -> {
            rdd.collect();
            logger.info("===================================");
            logger.info(" Number of records in this batch: " + rdd.count());
            logger.info("===================================");
        };
        lines.foreachRDD(voidFunction2);
    }

    private String getMaster() {
        return properties.getProperty("cluster.master");
    }

    private long getDurationsMilliseconds() {
        return Long.parseLong(properties.getProperty("streaming.durations.milliseconds"));
    }
}