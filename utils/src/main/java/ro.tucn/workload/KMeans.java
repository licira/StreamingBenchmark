package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.kafka.KafkaConsumerCustom;
import ro.tucn.operator.ContextCreator;
import ro.tucn.operator.StreamOperator;

/**
 * Created by Liviu on 4/15/2017.
 */
public class KMeans extends Workload {

    private static final Logger logger = Logger.getLogger(KMeans.class);
    private final KafkaConsumerCustom kafkaConsumerCustom;

    public KMeans(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
    }

    public void process() {/*PI*/
        kafkaConsumerCustom.setParallelism(parallelism);
        StreamOperator<Point> points = kafkaConsumerCustom.getPointOperator(properties, "topic1");
        StreamOperator<Point> centroids = kafkaConsumerCustom.getPointOperator(properties, "topic2");
        //points.print();
        //centroids.print();
        try {
            points.kMeansCluster(centroids);
        } catch (WorkloadException e) {
            e.printStackTrace();
        }
    }
}
