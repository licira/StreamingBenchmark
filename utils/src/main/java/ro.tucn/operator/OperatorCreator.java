package ro.tucn.operator;

import ro.tucn.kMeans.Point;
import ro.tucn.util.WithTime;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class OperatorCreator implements Serializable {

    protected String appName;

    public OperatorCreator(String appName) {
        this.appName = appName;
    }

    public abstract Operator<String> getStringStreamFromKafka(Properties properties,
                                                                   String topicPropertyName,
                                                                   String componentId,
                                                                   int parallelism);

    public abstract PairOperator<String, String> getPairStreamFromKafka(Properties properties,
                                                              String topicPropertyName,
                                                              String componentId,
                                                              int parallelism);

    public abstract Operator<WithTime<String>> getStringStreamWithTimeFromKafka(Properties properties,
                                                                                     String topicPropertyName,
                                                                                     String componentId,
                                                                                     int parallelism);

    public abstract Operator<Point> getPointStreamFromKafka(Properties properties,
                                                                 String topicPropertyName,
                                                                 String componentId,
                                                                 int parallelism);

    public abstract void Start();
}