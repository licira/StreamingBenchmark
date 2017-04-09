package ro.tucn.operator;

import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.util.TimeDuration;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
abstract public class WorkloadOperator<T> extends BaseOperator {

    public WorkloadOperator(int parallelism) {
        super(parallelism);
    }

    /**
     * Map T to R for each entity
     */
    abstract public <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId);

    /**
     * Map for iterative operator only
     */
    abstract public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException;

    abstract public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException;

    /**
     * Map T to Pair<K,V>, return PairWorkloadOperator
     */
    abstract public <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

    /**
     * reduce on whole stream
     */
    abstract public WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    /**
     * filter entity if fun(entity) is false
     */
    abstract public WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId);

    /**
     * Map T to iterable<R>
     */
    abstract public <R> WorkloadOperator<R> flatMap(FlatMapFunction<T, R> fun, String componentId);

    /**
     * Map T to Pair<K,V>, return PairWorkloadOperator
     */
    abstract public <K, V> PairWorkloadOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun, String componentId);

    abstract public WindowedWorkloadOperator<T> window(TimeDuration windowDuration);

    abstract public WindowedWorkloadOperator<T> window(TimeDuration windowDuration, TimeDuration slideDuration);

    abstract public void sink();
}