package ro.tucn.workload.stream;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractKafkaConsumerCustom;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.StreamOperator;
import ro.tucn.workload.AbstractKMeans;

/**
 * Created by Liviu on 4/15/2017.
 */
public class KMeansStream extends AbstractKMeans {

    private static final Logger logger = Logger.getLogger(KMeansStream.class);
    private final AbstractKafkaConsumerCustom kafkaConsumerCustom;

    public KMeansStream(ContextCreator creator) throws WorkloadException {
        super(creator);
        kafkaConsumerCustom = creator.getKafkaConsumerCustom();
        kafkaConsumerCustom.setParallelism(parallelism);
    }

    public void process() {
        StreamOperator<Point> points = kafkaConsumerCustom.getPointOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        StreamOperator<Point> centroids = kafkaConsumerCustom.getPointOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        try {
            super.process(points, centroids);
        } catch (WorkloadException e) {
            logger.error(e.getMessage());
        }
    }
}
