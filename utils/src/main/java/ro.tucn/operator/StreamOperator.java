package ro.tucn.operator;

import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.util.TimeDuration;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamOperator<T> extends Operator {

    public StreamOperator(int parallelism) {
        super(parallelism);
    }

    public abstract StreamPairOperator<String, Integer> wordCount();

    public abstract void kMeansCluster(StreamOperator<T> centroids) throws WorkloadException;

    /**
     * Map T to R for each entity
     */
    public abstract <R> StreamOperator<R> map(MapFunction<T, R> fun, String componentId);

    /**
     * Map for iterative operator only
     */
    public abstract <R> StreamOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException;

    public abstract <R> StreamOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException;

    /**
     * Map T to Pair<K,V>, return StreamPairOperator
     */
    public abstract <K, V> StreamPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

    /**
     * reduce on whole stream
     */
    public abstract StreamOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    /**
     * filter entity if fun(entity) is false
     */
    public abstract StreamOperator<T> filter(FilterFunction<T> fun, String componentId);

    /**
     * Map T to iterable<R>
     */
    public abstract <R> StreamOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId);

    public abstract StreamWindowedOperator<T> window(TimeDuration windowDuration);

    public abstract StreamWindowedOperator<T> window(TimeDuration windowDuration, TimeDuration slideDuration);

    public abstract void sink();

    public abstract StreamOperator map(StreamOperator<T> points);
}