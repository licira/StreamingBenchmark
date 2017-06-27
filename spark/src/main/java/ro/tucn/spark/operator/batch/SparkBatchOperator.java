package ro.tucn.spark.operator.batch;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.BatchPairOperator;

import java.util.function.Consumer;

/**
 * Created by Liviu on 6/25/2017.
 */
public class SparkBatchOperator<T> extends BatchOperator<T> {

    private static final Logger logger = Logger.getLogger(SparkBatchOperator.class);

    JavaRDD<T> rdd;

    public SparkBatchOperator(int parallelism) {
        super(parallelism);
    }

    @Override
    public BatchPairOperator<String, Integer> wordCount() {
        return null;
    }

    @Override
    public void kMeansCluster(BatchOperator<T> centroids) throws WorkloadException {

    }

    public SparkBatchOperator(JavaRDD<T> rdd, int parallelism) {
        super(parallelism);
        this.rdd = rdd;
    }


    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        rdd.collect().forEach(new Consumer<T>() {
            @Override
            public void accept(T t) {
                logger.info(t.toString());
            }
        });
    }
}
