package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;

/**
 * Created by Liviu on 6/25/2017.
 */
public abstract class BatchOperator<T> extends BaseOperator {

    public BatchOperator(int parallelism) {
        super(parallelism);
    }

    public abstract BatchPairOperator<String, Integer> wordCount();

    public abstract void kMeansCluster(BatchOperator<T> centroids) throws WorkloadException;
}
