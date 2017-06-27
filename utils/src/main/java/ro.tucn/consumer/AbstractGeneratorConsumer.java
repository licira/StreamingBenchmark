package ro.tucn.consumer;

import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class AbstractGeneratorConsumer extends AbstractConsumer {

    public abstract BatchPairOperator<String, String> getBatchPairOperator(Properties properties,
                                                                           String topicPropertyName);

    public abstract BatchOperator<TimeHolder<String>> getBatchStringOperatorWithTimeHolder(Properties properties,
                                                                                           String topicPropertyName);

    public abstract BatchOperator<Point> getBatchPointOperator(Properties properties,
                                                               String topicPropertyName);

    public abstract BatchOperator<String> getBatchStringOperator(Properties properties,
                                                                 String topicPropertyName);
}
