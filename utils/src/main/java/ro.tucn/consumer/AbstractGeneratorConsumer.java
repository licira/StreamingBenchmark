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

    public abstract BatchOperator<String> getStringOperator(Properties properties,
                                                            String topicPropertyName);

    public abstract BatchPairOperator<String, String> getPairOperator(Properties properties,
                                                                      String topicPropertyName);

    public abstract BatchOperator<Point> getPointOperator(Properties properties,
                                                          String topicPropertyName);

    public abstract void askGeneratorToProduceData(String topic);

    public abstract BatchOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                                      String topicPropertyName);
}
