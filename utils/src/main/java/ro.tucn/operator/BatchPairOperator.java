package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.util.TimeDuration;

import static ro.tucn.DataMode.BATCH;

/**
 * Created by Liviu on 6/25/2017.
 */
public abstract class BatchPairOperator<K, V> extends PairOperator<K, V> {

    public BatchPairOperator(int parallelism) {
        super(parallelism);
        dataMode = BATCH;
    }

    public abstract <R> BatchPairOperator advClick(PairOperator<K, R> joinData,
                                              TimeDuration duration, TimeDuration joinDuration) throws WorkloadException;

}
