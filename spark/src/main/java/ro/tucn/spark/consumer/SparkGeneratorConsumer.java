package ro.tucn.spark.consumer;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import ro.tucn.DataMode;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.generator.creator.GeneratorCreator;
import ro.tucn.generator.generator.AbstractGenerator;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.spark.operator.batch.SparkBatchOperator;
import ro.tucn.spark.operator.batch.SparkBatchPairOperator;
import ro.tucn.util.Message;
import ro.tucn.util.TimeHolder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkGeneratorConsumer extends AbstractGeneratorConsumer {

    private static final Logger logger = Logger.getLogger(SparkGeneratorConsumer.class);
    protected AbstractGenerator generator;
    private JavaSparkContext sc;
    private JavaStreamingContext jssc;

    public SparkGeneratorConsumer(JavaStreamingContext jssc, JavaSparkContext sc) {
        super();
        this.jssc = jssc;
        this.sc = sc;
    }

    @Override
    public void askGeneratorToProduceData(String topic) {
        generator = GeneratorCreator.getNewGenerator(topic, DataMode.BATCH, 0);
        generator.generate(0);
    }

    @Override
    public BatchOperator<String> getStringOperator(Properties properties,
                                                   String topicPropertyName) {
        JavaRDD<String> rddWithJsonAsValue = getStringWithJsonAsValueRddFromGenerator(properties, topicPropertyName);
        JavaRDD<String> rdd = getRddFromRddWithJsonAsValue(rddWithJsonAsValue);
        return new SparkBatchOperator<String>(rdd, parallelism);
    }

    @Override
    public BatchPairOperator<String, String> getPairOperator(Properties properties,
                                                             String topicPropertyName) {
        JavaRDD<String> streamWithJsonAsValue = getStringWithJsonAsValueRddFromGenerator(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaPairRDD<String, String> pairRdd = getPairRddFromRddWithJsonAsValue(streamWithJsonAsValue);
        return new SparkBatchPairOperator<String, String>(pairRdd, parallelism);
    }

    @Override
    public BatchOperator<Point> getPointOperator(Properties properties,
                                                 String topicPropertyName) {
        JavaRDD<String> jsonStream = getStringRddFromGenerator(properties, topicPropertyName);
        //jsonStream.print();
        JavaRDD<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new SparkBatchOperator<Point>(pointStream, parallelism);
    }

    @Override
    public BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                             String topicPropertyName) {
        return null;
    }

    private JavaRDD<Point> getPointStreamFromJsonStream(JavaRDD<String> jsonStream) {
        JavaRDD<Point> pointStream = jsonStream.map(s -> {
            Gson gson = new Gson();
            Point point = gson.fromJson(s, Point.class);
            return point;
        });
        return pointStream;
    }

    private JavaRDD<String> getStringRddFromGenerator(Properties properties, String topicPropertyName) {
        JavaRDD<String> rddWithJsonAsValue = getStringWithJsonAsValueRddFromKafka(properties, topicPropertyName);
        //streamWithJsonAsValue.print();
        JavaRDD<String> rdd = getRddFromRddWithJsonAsValue(rddWithJsonAsValue);
        return rdd;
    }

    private JavaRDD<String> getStringWithJsonAsValueRddFromKafka(Properties properties, String topicPropertyName) {
        JavaRDD<String> rddWithJsonAsValue = getDirectRddFromGenerator(properties, topicPropertyName);
        return rddWithJsonAsValue;
    }

    private JavaPairRDD<String, String> getPairRddFromRddWithJsonAsValue(JavaRDD<String> rddWithJsonAsValue) {
        JavaPairRDD<String, String> pairRdd = rddWithJsonAsValue.mapToPair(s -> {
            Gson gson = new Gson();
            Message msg = gson.fromJson(s, Message.class);
            return new Tuple2<String, String>(msg.getKey(), msg.getValue());
        });
        return pairRdd;
    }

    private JavaRDD<String> getRddFromRddWithJsonAsValue(JavaRDD<String> rddWithJsonAsValue) {
        JavaRDD<String> rdd = rddWithJsonAsValue.map(s -> {
            Gson gson = new Gson();
            Message msg = gson.fromJson(s, Message.class);
            return msg.getValue();
        });
        return rdd;
    }

    private JavaRDD<String> getStringWithJsonAsValueRddFromGenerator(Properties properties, String topicPropertyName) {
        JavaRDD<String> rdd = getDirectRddFromGenerator(properties, topicPropertyName);
        return rdd;
    }

    private JavaRDD<String> getDirectRddFromGenerator(Properties properties, String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        List<Map<String, String>> generatedData = generator.getGeneratedData(topic);
        List<String> jsons = new ArrayList<>();
        try {
            for (Map<String, String> map : generatedData) {
                String json = map.get(null);
                jsons.add(json);
            }
        } catch (NullPointerException e) {
            logger.error(e.getMessage());
        }
        JavaRDD<String> jsonRdds = sc.parallelize(jsons);
        return jsonRdds;
    }

    private String getTopicFromProperties(Properties properties, String topicPropertyName) {
        return properties.getProperty(topicPropertyName);
    }
}
