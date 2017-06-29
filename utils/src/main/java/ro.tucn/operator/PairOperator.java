package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.AssignTimeFunction;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

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
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param joinWindowDuration window length of joinStream
     * @param <R>                Value type of joinStream
     * @return joined stream
     */
     public abstract <R> PairOperator<K, Tuple2<V, R>> join(PairOperator<K, R> joinStream,
                                                            TimeDuration windowDuration, TimeDuration joinWindowDuration) throws WorkloadException;

    /**
     * Join two pair streams which have the same type of key -- K base on event time
     *
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param slideWindowDuration    window length of joinStream
     * @param <R>                Value type of joinStream
     * @param eventTimeAssigner1 event time assignment for this stream
     * @param eventTimeAssigner2 event time assignment for joinStream
     * @return joined stream
     */
    public abstract <R> PairOperator<K, Tuple2<V, R>> join(
            String componentId, PairOperator<K, R> joinStream,
            TimeDuration windowDuration, TimeDuration slideWindowDuration,
            AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException;

    protected void checkWindowDurationsCompatibility(TimeDuration duration1, TimeDuration duration2) throws WorkloadException {
        if (duration1.toMilliSeconds() % duration2.toMilliSeconds() != 0) {
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }
    }
}
