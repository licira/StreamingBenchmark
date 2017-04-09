package ro.tucn.operator;

import ro.tucn.kMeans.Point;
import ro.tucn.util.WithTime;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class OperatorCreator {

    private String appName;

    public OperatorCreator(String appName) {
        this.appName = appName;
    }

    /**
     * zkConStr: zoo1:2181
     * topics: Topic1,Topic2
     * offset smallest
     **/
    public abstract WorkloadOperator<WithTime<String>> stringStreamFromKafkaWithTime(String zkConStr,
                                                                                     String kafkaServers,
                                                                                     String group,
                                                                                     String topics,
                                                                                     String offset,
                                                                                     String componentId,
                                                                                     int parallelism);

    /**
     * Consume point stream from kafka for workload 3
     */
    public abstract WorkloadOperator<Point> pointStreamFromKafka(String zkConStr,
                                                                 String kafkaServers,
                                                                 String group,
                                                                 String topics,
                                                                 String offset,
                                                                 String componentId,
                                                                 int parallelism);

    public abstract WorkloadOperator<String> stringStreamFromKafka(String zkConStr,
                                                                   String kafkaServers,
                                                                   String group,
                                                                   String topics,
                                                                   String offset,
                                                                   String componentId,
                                                                   int parallelism);

    public String getAppName() {
        return this.appName;
    }

    /**
     * Start streaming analysis job
     */
    public abstract void Start();

}