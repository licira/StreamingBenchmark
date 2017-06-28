package ro.tucn.spark.consumer;

import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkGeneratorConsumer extends AbstractGeneratorConsumer {

    @Override
    public BatchPairOperator<String, String> getPairOperator(Properties properties, String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties, String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<Point> getPointOperator(Properties properties, String topicPropertyName) {
        return null;
    }

    @Override
    public BatchOperator<String> getStringOperator(Properties properties, String topicPropertyName) {
        return null;
    }
}
