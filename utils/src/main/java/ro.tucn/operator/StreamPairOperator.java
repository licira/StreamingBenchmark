package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.util.TimeDuration;

import static ro.tucn.DataMode.STREAMING;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamPairOperator<K, V> extends PairOperator<K, V> {

    public StreamPairOperator(int parallelism) {
        super(parallelism);
        dataMode = STREAMING;
    }

    public abstract <R> StreamPairOperator advClick(PairOperator<K, R> joinData,
                                                   TimeDuration duration, TimeDuration joinDuration) throws WorkloadException;
}

