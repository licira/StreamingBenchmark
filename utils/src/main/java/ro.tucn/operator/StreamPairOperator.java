package ro.tucn.operator;

import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.util.TimeDuration;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamPairOperator<K, V> extends PairOperator<K, V> {

    public StreamPairOperator(int parallelism) {
        super(parallelism);
    }

    /*
    public abstract GroupedOperator<K, V> groupByKey();

    // TODO: translate to reduce on each node, then group merge
    public abstract StreamPairOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);
    */
    /**
     * Map <K,V> tuple to <K,R>
     *
     * @param fun         map V to R
     * @param componentId component Id of this bolt
     * @param <R>
     * @return maped StreamPairOperator<K,R>
     */
    /*public abstract <R> StreamPairOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId);

    public abstract <R> StreamOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId);

    public abstract <R> StreamOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass);

    public abstract <R> StreamPairOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId);

    public abstract StreamPairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId);

    public abstract StreamPairOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    public abstract StreamPairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDuration windowDuration);
    */
    /**
     * Pre-aggregation -> key group -> reduce
     *
     * @param fun            reduce function
     * @param componentId
     * @param windowDuration
     * @param slideDuration
     * @return
     */
    /*public abstract StreamPairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                                  TimeDuration windowDuration, TimeDuration slideDuration);

    public abstract StreamWindowedPairOperator<K, V> window(TimeDuration windowDuration);

    public abstract StreamWindowedPairOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration);
    */
    public abstract void sink();

    public abstract void count();
}

