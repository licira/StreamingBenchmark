package ro.tucn.spark.operator;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.WorkloadOperator;
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

    private static Function<Tuple2<String, String>, WithTime<String>> mapFunctionWithTime
            = new Function<Tuple2<String, String>, WithTime<String>>() {
        public WithTime<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
            String[] list = stringStringTuple2._2().split(Constants.TimeSeparatorRegex);
            if (list.length == 2) {
                return new WithTime<String>(list[0], Long.parseLong(list[1]));
            }
            return new WithTime(stringStringTuple2._2(), System.currentTimeMillis());
        }
    };
    private static Function<Tuple2<String, String>, String> mapFunction
            = new Function<Tuple2<String, String>, String>() {
        public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
            return stringStringTuple2._2();
        }
    };
    public JavaStreamingContext jssc;
    private Properties properties;

    public SparkOperatorCreator(String appName) throws IOException {
        super(appName);
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("spark-cluster.properties"));
        SparkConf conf = new SparkConf().setMaster(this.getMaster()).setAppName(appName);
        conf.set("spark.streaming.ui.retainedBatches", "2000");
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(this.getDurationsMilliseconds()));
        /*
        JavaSparkContext sc = new JavaSparkContext(conf);
        jssc = new JavaStreamingContext(conf, Durations.milliseconds(this.getDurationsMilliseconds()));
        */
    }

    @Override
    public void Start() {
        jssc.addStreamingListener(new PerformanceStreamingListener());
//        jssc.checkpoint("/tmp/log-analyzer-streaming");
        // jssc.checkpoint("/tmp/spark/checkpoint");
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SparkWorkloadOperator<WithTime<String>> stringStreamFromKafkaWithTime(String zkConStr,
                                                                                 String kafkaServers,
                                                                                 String group,
                                                                                 String topics,
                                                                                 String offset,
                                                                                 String componentId,
                                                                                 int parallelism) {
        HashSet<String> topicsSet = new HashSet(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", kafkaServers);
        kafkaParams.put("auto.offset.reset", offset);
        kafkaParams.put("zookeeper.connect", zkConStr);
        kafkaParams.put("group.id", group);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<WithTime<String>> lines = messages.map(mapFunctionWithTime);

        return new SparkWorkloadOperator(lines, parallelism);
    }

    @Override
    public WorkloadOperator<Point> pointStreamFromKafka(String zkConStr, String kafkaServers, String group, String topics, String offset, String componentId, int parallelism) {
        return null;
    }

    @Override
    public WorkloadOperator<String> stringStreamFromKafka(String zkConStr,
                                                          String kafkaServers,
                                                          String group,
                                                          String topics,
                                                          String offset,
                                                          String componentId,
                                                          int parallelism) {
        HashSet<String> topicsSet = new HashSet(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", kafkaServers);
        kafkaParams.put("auto.offset.reset", offset);
        kafkaParams.put("zookeeper.connect", zkConStr);
        kafkaParams.put("group.id", group);

        // Create direct kafka stream with brokers and topics
        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
        VoidFunction2<JavaPairRDD<String, String>, Time> function2 = new VoidFunction2<JavaPairRDD<String, String>, Time>() {

            public void call(JavaPairRDD<String, String> newEventsRdd, Time time)
                    throws Exception {
                System.out.println("\n===================================");
                System.out.println("New Events for " + time + " batch:");
                for (Tuple2<String, String> tuple : newEventsRdd.collect()) {
                    System.out.println("tuples: " + tuple._1 + ": " + tuple._2);
                }
            }
        };
        messages.foreachRDD(function2);

        JavaDStream<String> lines = messages.map(mapFunction);
        VoidFunction2<JavaRDD<String>, Time> voidFunction2 = new VoidFunction2<JavaRDD<String>, Time>() {
            public void call(JavaRDD<String> rdd, Time time)
                    throws Exception {
                rdd.collect();
                System.out.println(" Number of records in this batch: "
                        + rdd.count());
            }
        };
        lines.foreachRDD(voidFunction2);
        return new SparkWorkloadOperator(lines, parallelism);
    }

    public String getMaster() {
        return properties.getProperty("cluster.master");
    }

    public long getDurationsMilliseconds() {
        return Long.parseLong(properties.getProperty("streaming.durations.milliseconds"));
    }
}