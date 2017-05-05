package ro.tucn.operator;

import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class PairWorkloadOperator<K, V> extends BaseOperator {

    public PairWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    public abstract GroupedWorkloadOperator<K, V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    public abstract PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * Map <K,V> tuple to <K,R>
     *
     * @param fun         map V to R
     * @param componentId component Id of this bolt
     * @param <R>
     * @return maped PairWorkloadOperator<K,R>
     */
    public abstract <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId);

    public abstract <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId);

    public abstract <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass);

    public abstract <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId);

    public abstract PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId);

    public abstract PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    public abstract PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDuration windowDuration);

    /**
     * Pre-aggregation -> key group -> reduce
     *
     * @param fun            reduce function
     * @param componentId
     * @param windowDuration
     * @param slideDuration
     * @return
     */
    public abstract PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                                    TimeDuration windowDuration, TimeDuration slideDuration);

    public abstract WindowedPairWorkloadOperator<K, V> window(TimeDuration windowDuration);

    public abstract WindowedPairWorkloadOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration);

    /**
     * Join two pair streams which have the same type of key -- K
     *
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param joinWindowDuration window length of joinStream
     * @param <R>                Value type of joinStream
     * @return joined stream
     */
    public abstract <R> PairWorkloadOperator<K, Tuple2<V, R>> join(
            String componentId, PairWorkloadOperator<K, R> joinStream,
            TimeDuration windowDuration, TimeDuration joinWindowDuration) throws WorkloadException;

    /**
     * Join two pair streams which have the same type of key -- K base on event time
     *
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param windowDuration2    window length of joinStream
     * @param <R>                Value type of joinStream
     * @param eventTimeAssigner1 event time assignment for this stream
     * @param eventTimeAssigner2 event time assignment for joinStream
     * @return joined stream
     */
    public abstract <R> PairWorkloadOperator<K, Tuple2<V, R>> join(
            String componentId, PairWorkloadOperator<K, R> joinStream,
            TimeDuration windowDuration, TimeDuration windowDuration2,
            AssignTimeFunction<V> eventTimeAssigner1, AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException;

    public abstract void sink();
}

