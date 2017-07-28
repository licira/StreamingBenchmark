package ro.tucn.spark.context;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import ro.tucn.DataMode;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.spark.consumer.SparkGeneratorConsumer;
import ro.tucn.spark.consumer.SparkKafkaConsumerCustom;
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

    public SparkContextCreator(String appName, String dataMode, int parallelism) throws IOException {
        super(appName);
        initializeProperties();
        this.parallelism = parallelism;
        this.dataMode = dataMode;
        this.frameworkName = "SPARK";
        initializeJavaStreamingContext(appName);
    }

    @Override
    public void start() {
        if (dataMode.equals(DataMode.STREAMING)) {
            jssc.addStreamingListener(new PerformanceStreamingListener());
            //jssc.checkpoint("/tmp/spark/checkpoint");
            jssc.start();
            try {
                jssc.awaitTermination();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public AbstractKafkaConsumerCustom getKafkaConsumerCustom() {
        return new SparkKafkaConsumerCustom(jssc, sc);
    }

    @Override
    public AbstractGeneratorConsumer getGeneratorConsumer() {
        return new SparkGeneratorConsumer(jssc, sc);
    }

    private void initializeProperties() throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(SPARK_PROPERTIES_FILE_NAME));
        if ((parallelism == 0) || (parallelism == -1)) {
            try {
                parallelism = Integer.parseInt(String.valueOf(properties.get("parallelism")));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            if (parallelism == -1) {
                parallelism = 1;
            }
        }
    }

    private void initializeJavaStreamingContext(String appName) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .set("spark.executor.instances", String.valueOf(parallelism))
                //.set("executor.cores", "1")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                ;
        if (dataMode.equals(DataMode.BATCH)) {
            conf.setMaster("local[" + parallelism + "]");
        } else if (dataMode.equals(DataMode.STREAMING)) {
            conf.setMaster(getMaster());
        }

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