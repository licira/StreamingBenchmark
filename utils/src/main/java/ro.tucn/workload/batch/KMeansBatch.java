package ro.tucn.workload.batch;

import org.apache.log4j.Logger;
import ro.tucn.consumer.AbstractGeneratorConsumer;
import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BatchOperator;
import ro.tucn.workload.Workload;

/**
 * Created by Liviu on 6/27/2017.
 */
public class KMeansBatch extends Workload {

    private static final Logger logger = Logger.getLogger(KMeansBatch.class);
    private final AbstractGeneratorConsumer generatorConsumer;

    public KMeansBatch(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        generatorConsumer = contextCreator.getGeneratorConsumer();
    }

    @Override
    public void process() {
        generatorConsumer.setParallelism(parallelism);
        BatchOperator<Point> points = generatorConsumer.getPointOperator(properties, "topic1");
        BatchOperator<Point> centroids = generatorConsumer.getPointOperator(properties, "topic2");
        //points.print();
        //centroids.print();
        try {
            points.kMeansCluster(centroids);
        } catch (WorkloadException e) {
            e.printStackTrace();
        }
    }
}
