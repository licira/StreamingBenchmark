package ro.tucn.kafka;

import ro.tucn.kMeans.Point;
import ro.tucn.operator.Operator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeHolder;

import java.util.Properties;

/**
 * Created by Liviu on 6/24/2017.
 */
public abstract class KafkaConsumerCustom {

    protected int parallelism;

    public abstract Operator<String> getStringOperator(Properties properties,
                                                              String topicPropertyName);

    public abstract PairOperator<String, String> getPairOperator(Properties properties,
                                                                        String topicPropertyName);

    public abstract Operator<TimeHolder<String>> getStringOperatorTimeHolder(Properties properties,
                                                                                String topicPropertyName);

    public abstract Operator<Point> getPointOperator(Properties properties,
                                                            String topicPropertyName);

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }
}
