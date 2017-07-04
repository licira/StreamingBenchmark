package ro.tucn.workload;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.Operator;

/**
 * Created by Liviu on 7/4/2017.
 */
public abstract class AbstractKMeans extends AbstractWorkload {

    public AbstractKMeans(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
    }

    public void process(Operator<Point> points, Operator<Point> centroids) throws WorkloadException {
        points.kMeansCluster(centroids);
    }
}