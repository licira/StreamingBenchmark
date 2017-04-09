package ro.tucn.operator;

import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
abstract public class WindowedPairWorkloadOperator<K, V> extends BaseOperator {

    public WindowedPairWorkloadOperator(int parallelism) {
        super(parallelism);
    }

    /**
     * @param fun
     * @param componentId
     * @return PairWorkloadOperator<K, V>
     */
    abstract public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * cumulate window stream
     *
     * @param fun
     * @param componentId
     * @return
     */
    abstract public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    // return WorkloadOperator<R>
    abstract public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId);

    // return new WorkloadOperator<R>();
    abstract public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId);

    // return new WorkloadOperator<T>();
    abstract public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId);

    abstract public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId);

}
