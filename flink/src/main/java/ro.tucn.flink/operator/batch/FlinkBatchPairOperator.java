package ro.tucn.flink.operator.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
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
public class FlinkBatchPairOperator<K, V> extends BatchPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(FlinkBatchPairOperator.class);

    private DataSet<Tuple2<K, V>> dataSet;

    public FlinkBatchPairOperator(DataSet<Tuple2<K, V>> dataSet, int parallelism) {
        super(parallelism);
        this.dataSet = dataSet;
    }

    @Override
    public <R> PairOperator<K, Tuple2<V, R>> join(PairOperator<K, R> joinStream, TimeDuration windowDuration, TimeDuration joinWindowDuration) throws WorkloadException {
        return null;
    }


    @Override
    public <R> PairOperator<K, Tuple2<V, R>> join(String componentId,
                                                  PairOperator<K, R> joinStream,
                                                  TimeDuration windowDuration,
                                                  TimeDuration joinWindowDuration,
                                                  final AssignTimeFunction<V> eventTimeAssigner1,
                                                  final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {

    }
}
