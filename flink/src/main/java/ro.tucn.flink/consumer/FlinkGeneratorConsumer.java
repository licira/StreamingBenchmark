package ro.tucn.flink.consumer;

import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkGeneratorConsumer extends AbstractGeneratorConsumer {

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
