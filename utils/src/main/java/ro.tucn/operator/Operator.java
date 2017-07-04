package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class Operator<T> extends BaseOperator {

    public Operator(int parallelism) {
        super(parallelism);
    }

    public abstract PairOperator<String,Integer> wordCount();

    public abstract void kMeansCluster(Operator<T> centroids) throws WorkloadException;
}
