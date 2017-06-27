package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.ContextCreator;
import ro.tucn.spark.kafka.SparkKafkaConsumerCustom;
import ro.tucn.spark.statistics.PerformanceStreamingListener;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkContextCreator extends ContextCreator {

    private static final Logger logger = Logger.getLogger(SparkContextCreator.class);

    private static final String TOPIC_SPLITTER = ",";
    private static final String SPARK_PROPERTIES_FILE_NAME = "spark-cluster.properties";

    public static JavaStreamingContext jssc;
    public static JavaSparkContext sc;
    private Properties properties;

    public SparkContextCreator(String appName) throws IOException {
        super(appName);
        initializeProperties();
        initializeJavaStreamingContext(appName);
    }

    @Override
    public void Start() {
        jssc.addStreamingListener(new PerformanceStreamingListener());
        //jssc.checkpoint("/tmp/spark/checkpoint");
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public KafkaConsumerCustom getKafkaConsumerCustom() {
        return new SparkKafkaConsumerCustom(jssc, sc);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(SPARK_PROPERTIES_FILE_NAME));
    }

    private void initializeJavaStreamingContext(String appName) {
        SparkConf conf = new SparkConf()
                .setMaster(getMaster())
                .setAppName(appName)
                .set("spark.driver.memory", "256m")
                .set("spark.executor.memory", "768m")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.task.maxFailures", "5")
                //.set("spark.streaming.kafka.maxRetries", "5")
                .set("spark.streaming.ui.retainedBatches", "2000");
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