package ro.tucn.operator;

import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class WindowedPairOperator<K, V> extends BaseOperator {

    public WindowedPairOperator(int parallelism) {
        super(parallelism);
    }

    /**
     * @param fun
     * @param componentId
     * @return PairOperator<K, V>
     */
    public abstract PairOperator<K, V> reduceByKey(ReduceFunction<V> fun, String componentId);

    /**
     * cumulate window stream
     *
     * @param fun
     * @param componentId
     * @return
     */
    public abstract PairOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId);

    // return Operator<R>
    public abstract <R> PairOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId);

    // return new Operator<R>();
    public abstract <R> PairOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId);

    // return new Operator<T>();
    public abstract PairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId);

    public abstract PairOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun, String componentId);

}
