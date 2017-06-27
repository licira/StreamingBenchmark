package ro.tucn.flink.operator.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;

/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkBatchOperator<T> extends BatchOperator<T> {

    private static final Logger logger = Logger.getLogger(FlinkBatchOperator.class);

    DataSet<T> dataSet;

    public FlinkBatchOperator(int parallelism) {
        super(parallelism);
    }

    @Override
    public BatchPairOperator<String, Integer> wordCount() {
        return null;
    }

    @Override
    public void kMeansCluster(BatchOperator<T> centroids) throws WorkloadException {

    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {

    }
}
