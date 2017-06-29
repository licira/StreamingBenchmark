package ro.tucn.spark.consumer;

import com.google.gson.Gson;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkGeneratorConsumer extends AbstractGeneratorConsumer {

    private AbstractGenerator generator;
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
        String topic = getTopicFromProperties(properties, topicPropertyName);
        JavaRDD<String> rddWithJsonAsValue = getStringWithJsonAsValueRddFromGenerator(topic);
        JavaRDD<String> rdd = getRddFromRddWithJsonAsValue(rddWithJsonAsValue);
        return new SparkBatchOperator<String>(rdd, parallelism);
    }

    @Override
    public BatchPairOperator<String, String> getPairOperator(Properties properties,
                                                             String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        JavaRDD<String> streamWithJsonAsValue = getStringWithJsonAsValueRddFromGenerator(topic);
        JavaPairRDD<String, String> pairRdd = getPairRddFromRddWithJsonAsValue(streamWithJsonAsValue);
        return new SparkBatchPairOperator<String, String>(pairRdd, parallelism);
    }

    @Override
    public BatchOperator<Point> getPointOperator(Properties properties,
                                                 String topicPropertyName) {
        String topic = getTopicFromProperties(properties, topicPropertyName);
        JavaRDD<String> jsonStream = getStringRddFromGenerator(topic);
        JavaRDD<Point> pointStream = getPointStreamFromJsonStream(jsonStream);
        return new SparkBatchOperator<Point>(pointStream, parallelism);
    }

    @Override
    public BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                             String topicPropertyName) {
        return null;
    }

    private JavaRDD<Point> getPointStreamFromJsonStream(JavaRDD<String> jsonStream) {
        return jsonStream.map(s -> {
            Gson gson = new Gson();
            Point point = gson.fromJson(s, Point.class);
            return point;
        });
    }

    private JavaRDD<String> getStringRddFromGenerator(String topic) {
        JavaRDD<String> rddWithJsonAsValue = getStringWithJsonAsValueRddFromKafka(topic);
        return getRddFromRddWithJsonAsValue(rddWithJsonAsValue);
    }

    private JavaRDD<String> getStringWithJsonAsValueRddFromKafka(String topic) {
        return getDirectRddFromGenerator(topic);
    }

    private JavaPairRDD<String, String> getPairRddFromRddWithJsonAsValue(JavaRDD<String> rddWithJsonAsValue) {
        return rddWithJsonAsValue.mapToPair(s -> {
            Gson gson = new Gson();
            Message msg = gson.fromJson(s, Message.class);
            return new Tuple2<String, String>(msg.getKey(), msg.getValue());
        });
    }

    private JavaRDD<String> getRddFromRddWithJsonAsValue(JavaRDD<String> rddWithJsonAsValue) {
        return rddWithJsonAsValue.map(s -> {
            Gson gson = new Gson();
            Message msg = gson.fromJson(s, Message.class);
            return msg.getValue();
        });
    }

    private JavaRDD<String> getStringWithJsonAsValueRddFromGenerator(String topic) {
        return getDirectRddFromGenerator(topic);
    }

    private JavaRDD<String> getDirectRddFromGenerator(String topic) {
        List<Map<String, String>> generatedData = generator.getGeneratedData(topic);
        List<String> jsons = getJsonListFromMapList(generatedData);
        return sc.parallelize(jsons);
    }
}
