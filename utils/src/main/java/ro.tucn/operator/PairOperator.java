package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.util.TimeDuration;

/**
 * Created by Liviu on 6/27/2017.
 */
public abstract class PairOperator<K, V> extends BaseOperator {

    public PairOperator(int parallelism) {
        super(parallelism);
    }

    /**
     * Join two pair streams which have the same type of key -- K
     *
     * @param joinData        the other data<K,R>
     * @param windowDuration     window length of this data
     * @param joinWindowDuration window length of joinData
     * @param <R>                Value type of joinStream
     * @return joined data
     */
    public abstract <R> PairOperator advClick(PairOperator<K, R> joinData,
                                              TimeDuration windowDuration, TimeDuration joinWindowDuration) throws WorkloadException;

    protected void checkWindowDurationsCompatibility(TimeDuration duration1, TimeDuration duration2) throws WorkloadException {
        if (duration1.toMilliSeconds() % duration2.toMilliSeconds() != 0) {
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }
    }
}
