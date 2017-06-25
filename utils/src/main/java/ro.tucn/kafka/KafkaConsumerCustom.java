package ro.tucn.kafka;

import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
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

    public abstract Operator<TimeHolder<String>> getStringOperatorWithTimeHolder(Properties properties,
                                                                                String topicPropertyName);

    public abstract Operator<Point> getPointOperator(Properties properties,
                                                            String topicPropertyName);

    public abstract BatchOperator<String> getBatchStringOperator(Properties properties,
                                                                 String topicPropertyName);

    /*public abstract BatchPairOperator<String, String> getBatchPairOperator(Properties properties,
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
