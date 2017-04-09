package ro.tucn.spark.function.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.spark.util.Utils;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by Liviu on 4/8/2017.
 */
public class PairMapPartitionFunctionImpl<K, V, R> implements PairFlatMapFunction<Iterator<Tuple2<K, V>>, K, R> {

    MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun;

    public PairMapPartitionFunctionImpl(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> function) {
        this.fun = function;
    }

    public Iterable<Tuple2<K, R>> call(Iterator<Tuple2<K, V>> tuple2Iterator) throws Exception {
        return fun.mapPartition(Utils.iterable(tuple2Iterator));
    }
}