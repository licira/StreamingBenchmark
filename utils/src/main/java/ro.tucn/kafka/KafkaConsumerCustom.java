package ro.tucn.kafka;

import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/24/2017.
 */
public abstract class KafkaConsumerCustom {

    protected int parallelism;

    public abstract StreamOperator<String> getStringOperator(Properties properties,
                                                              String topicPropertyName);

    public abstract StreamPairOperator<String, String> getStreamPairOperator(Properties properties,
                                                                        String topicPropertyName);

    public abstract StreamOperator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                                String topicPropertyName);

    public abstract StreamOperator<Point> getPointOperator(Properties properties,
                                                            String topicPropertyName);

    public abstract BatchOperator<String> getBatchStringOperator(Properties properties,
                                                                 String topicPropertyName);

    /*public abstract BatchStreamPairOperator<String, String> getBatchStreamPairOperator(Properties properties,
                                                                           String topicPropertyName);

    public abstract BatchOperator<TimeHolder<String>> getBatchStringOperatorWithTimeHolder(Properties properties,
                                                                                 String topicPropertyName);

    public abstract BatchOperator<Point> getBatchPointOperator(Properties properties,
                                                     String topicPropertyName);
    */
    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
