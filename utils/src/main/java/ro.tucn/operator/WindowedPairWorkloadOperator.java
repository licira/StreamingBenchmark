package ro.tucn.operator;

import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class WindowedPairWorkloadOperator<K, V> extends BaseOperator {

    public WindowedPairWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    /**
     * @param fun
     * @param componentId
     * @return PairWorkloadOperator<K, V>
     */
    public abstract PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * cumulate window stream
     *
     * @param fun
     * @param componentId
     * @return
     */
    public abstract PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    // return WorkloadOperator<R>
    public abstract <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId);

    // return new WorkloadOperator<R>();
    public abstract <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId);

    // return new WorkloadOperator<T>();
    public abstract PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId);

    public abstract PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId);

}
