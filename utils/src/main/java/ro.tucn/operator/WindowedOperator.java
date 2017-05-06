package ro.tucn.operator;

import ro.tucn.frame.functions.*;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class WindowedOperator<T> extends BaseOperator {

    public WindowedOperator(int parallelism) {
        super(parallelism);
    }

    // return Operator<R>
    public abstract <R> Operator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId);

    // return new Operator<R>();
    public abstract <R> Operator<R> map(MapFunction<T, R> fun, String componentId);

    // return new Operator<T>();
    public abstract Operator<T> filter(FilterFunction<T> fun, String componentId);

    // return new Operator<T>();
    public abstract Operator<T> reduce(ReduceFunction<T> fun, String componentId);

    // return new PairOperator<K,V>
    public abstract <K, V> PairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

}
