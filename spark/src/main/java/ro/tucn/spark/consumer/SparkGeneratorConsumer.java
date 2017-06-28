package ro.tucn.spark.consumer;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import ro.tucn.DataMode;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.generator.creator.GeneratorCreator;
import ro.tucn.generator.generator.AbstractGenerator;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.TimeHolder;

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
    public BatchPairOperator<String, String> getPairOperator(Properties properties,
                                                             String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                             String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<Point> getPointOperator(Properties properties,
                                                 String topicPropertyName) {
        return null;
    }

    @Override
    public void askGeneratorToProduceData(String topic) {
        generator = GeneratorCreator.getNewGenerator(topic, DataMode.BATCH, 0);
        generator.generate(0);
    }

    @Override
    public BatchOperator<String> getStringOperator(Properties properties,
                                                   String topicPropertyName) {
        return null;
    }
}
