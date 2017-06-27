package ro.tucn.spark.operator;

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

/**
 * Created by Liviu on 6/27/2017.
 */
public class SparkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(SparkBatchPairOperator.class);

    public JavaPairRDD<K, V> pairRDD;

    public SparkBatchPairOperator(int parallelism) {
        super(parallelism);
    }

    @Override
    public PairOperator join(String componentId, PairOperator joinStream, TimeDuration windowDuration, TimeDuration windowDuration2, AssignTimeFunction eventTimeAssigner1, AssignTimeFunction eventTimeAssigner2) throws WorkloadException {
        return null;
    }

    @Override
    public <R> PairOperator<K, Tuple2<V, R>> join(PairOperator<K, R> joinOperator,
                                                  TimeDuration windowDuration,
                                                  TimeDuration joinWindowDuration) throws WorkloadException {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {

    }

}
