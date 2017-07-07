package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class Operator<T> extends BaseOperator {

    public Operator(int parallelism) {
        super(parallelism);
    }

    public abstract PairOperator<String,Integer> wordCount();

    public abstract PairOperator<Point, Integer> kMeansCluster(Operator<T> centroids, int numIterations) throws WorkloadException;
}
