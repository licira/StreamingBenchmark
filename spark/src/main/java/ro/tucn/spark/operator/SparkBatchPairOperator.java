package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import ro.tucn.operator.BatchPairOperator;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(SparkBatchPairOperator.class);

    public JavaPairRDD<K, V> pairRDD;

    public SparkBatchPairOperator(int parallelism) {
        super(parallelism);
    }
}
