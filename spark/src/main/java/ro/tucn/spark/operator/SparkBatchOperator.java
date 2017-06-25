package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchOperator;

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
