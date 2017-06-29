package ro.tucn.spark.operator.batch;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.AssignTimeFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchPairOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.util.List;

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(SparkBatchPairOperator.class);

    public JavaPairRDD<K, V> pairRDD;

    public SparkBatchPairOperator(JavaPairRDD<K, V> pairRDD, int parallelism) {
        super(parallelism);
        this.pairRDD = pairRDD;
    }

    @Override
    public PairOperator join(String componentId, PairOperator joinStream, TimeDuration windowDuration, TimeDuration windowDuration2, AssignTimeFunction eventTimeAssigner1, AssignTimeFunction eventTimeAssigner2) throws WorkloadException {
        return null;
    }

    @Override
    public <R> PairOperator<K, Tuple2<V, R>> advClick(PairOperator<K, R> joinOperator,
                                                  TimeDuration windowDuration,
                                                  TimeDuration joinWindowDuration) throws WorkloadException {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        List<Tuple2<K, V>> collect = pairRDD.collect();
        for (Tuple2<K, V> tuple : collect) {
            logger.info(tuple);
        }
    }

}
