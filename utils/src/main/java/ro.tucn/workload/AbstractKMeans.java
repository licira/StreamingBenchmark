package ro.tucn.workload;

import ro.tucn.context.ContextCreator;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.Operator;
import ro.tucn.util.TimeDuration;

/**
 * Created by Liviu on 7/4/2017.
 */
public abstract class AbstractKMeans extends AbstractWorkload {

    private static final String numIterationsPropertyName = "num.iterations";

    public AbstractKMeans(ContextCreator contextCreator) throws WorkloadException {
        super(contextCreator);
        workloadName = "K-Means";
    }

    public void process(Operator<Point> points, Operator<Point> centroids) throws WorkloadException {
        int numIterations = Integer.parseInt(properties.getProperty(numIterationsPropertyName));
        points.kMeansCluster(centroids, numIterations);
        points.printExecutionLatency();

        long latency = points.getExecutionLatency();

        performanceLog.logToCsv(points.getFrameworkName(), workloadName, points.getDataMode(), String.valueOf(TimeDuration.nanosToSeconds(latency)), null);
    }
}
