package ro.tucn.consumer;

import ro.tucn.kMeans.Point;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/24/2017.
 */
public abstract class KafkaConsumerCustom extends AbstractConsumer {

    public abstract StreamOperator<String> getStringOperator(Properties properties,
                                                              String topicPropertyName);

    public abstract StreamPairOperator<String, String> getStreamPairOperator(Properties properties,
                                                                        String topicPropertyName);

    public abstract StreamOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                                String topicPropertyName);

    public abstract StreamOperator<Point> getPointOperator(Properties properties,
                                                            String topicPropertyName);
}
