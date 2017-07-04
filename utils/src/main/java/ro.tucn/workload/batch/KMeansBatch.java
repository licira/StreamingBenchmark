package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.topic.ApplicationTopics;
import ro.tucn.workload.AbstractWorkload;

/**
 * Created by Liviu on 6/27/2017.
 */
public class KMeansBatch extends AbstractWorkload {

    private static final Logger logger = Logger.getLogger(KMeansBatch.class);
    private final AbstractGeneratorConsumer generatorConsumer;

    public KMeansBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        generatorConsumer = contextCreator.getGeneratorConsumer();
        generatorConsumer.setParallelism(parallelism);
        generatorConsumer.askGeneratorToProduceData(ApplicationTopics.K_MEANS);
    }

    @Override
    public void process() {
        BatchOperator<Point> points = generatorConsumer.getPointOperator(properties, TOPIC_ONE_PROPERTY_NAME);
        BatchOperator<Point> centroids = generatorConsumer.getPointOperator(properties, TOPIC_TWO_PROPERTY_NAME);
        points.print();
        centroids.print();
        try {
            points.kMeansCluster(centroids);
        } catch (WorkloadException e) {
            e.printStackTrace();
        }
    }
}
