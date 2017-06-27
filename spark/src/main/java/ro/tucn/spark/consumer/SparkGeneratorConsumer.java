package ro.tucn.spark.consumer;

import ro.tucn.consumer.GeneratorConsumer;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkGeneratorConsumer extends GeneratorConsumer {

    @Override
    public BatchPairOperator<String, String> getBatchPairOperator(Properties properties, String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<TimeHolder<String>> getBatchStringOperatorWithTimeHolder(Properties properties, String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<Point> getBatchPointOperator(Properties properties, String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<String> getBatchStringOperator(Properties properties, String topicPropertyName) {
        return null;
    }
}
