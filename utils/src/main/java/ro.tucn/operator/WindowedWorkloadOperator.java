package ro.tucn.operator;

import ro.tucn.frame.functions.*;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class WindowedWorkloadOperator<T> extends BaseOperator {

    public WindowedWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    // return WorkloadOperator<R>
    public abstract <R> WorkloadOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId);

    // return new WorkloadOperator<R>();
    public abstract <R> WorkloadOperator<R> map(MapFunction<T, R> fun, String componentId);

    // return new WorkloadOperator<T>();
    public abstract WorkloadOperator<T> filter(FilterFunction<T> fun, String componentId);

    // return new WorkloadOperator<T>();
    public abstract WorkloadOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    // return new PairWorkloadOperator<K,V>
    public abstract <K, V> PairWorkloadOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

}
