package ro.tucn.spark.function;

import org.apache.spark.api.java.function.PairFunction;
import ro.tucn.frame.functions.ReduceFunction;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class GrouperPairFunctionImpl<K, V> implements PairFunction<Tuple2<K, Iterable<V>>, K, V> {

    private ReduceFunction<V> function;

    public GrouperPairFunctionImpl(ReduceFunction<V> function) {
        this.function = function;
    }

    public Tuple2<K, V> call(Tuple2<K, Iterable<V>> kIterableTuple2) throws Exception {
        V reducedV = null;
        for (V v : kIterableTuple2._2()) {
            if (null == reducedV) {
                reducedV = v;
            } else {
                reducedV = function.reduce(reducedV, v);
            }
        }
        return new Tuple2(kIterableTuple2._1(), reducedV);
    }
}