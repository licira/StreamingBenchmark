package ro.tucn.operator;

import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.util.TimeDuration;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class Operator<T> extends BaseOperator {

    public Operator(int parallelism) {
        super(parallelism);
    }

    /**
     * Map T to R for each entity
     */
    public abstract <R> Operator<R> map(MapFunction<T, R> fun, String componentId);

    /**
     * Map for iterative operator only
     */
    public abstract <R> Operator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException;

    public abstract <R> Operator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException;

    /**
     * Map T to Pair<K,V>, return PairOperator
     */
    public abstract <K, V> PairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

    /**
     * reduce on whole stream
     */
    public abstract Operator<T> reduce(ReduceFunction<T> fun, String componentId);

    /**
     * filter entity if fun(entity) is false
     */
    public abstract Operator<T> filter(FilterFunction<T> fun, String componentId);

    /**
     * Map T to iterable<R>
     */
    public abstract <R> Operator<R> flatMap(FlatMapFunction<T, R> fun, String componentId);

    /**
     * Map T to Pair<K,V>, return PairOperator
     */
    public abstract <K, V> PairOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun, String componentId);

    public abstract WindowedOperator<T> window(TimeDuration windowDuration);

    public abstract WindowedOperator<T> window(TimeDuration windowDuration, TimeDuration slideDuration);

    public abstract void sink();
}