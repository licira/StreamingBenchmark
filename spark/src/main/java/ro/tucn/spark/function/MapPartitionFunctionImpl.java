package ro.tucn.spark.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.spark.util.Utils;

import java.util.Iterator;

/**
 * Created by Liviu on 4/8/2017.
 */
public class MapPartitionFunctionImpl<T, R> implements FlatMapFunction<Iterator<T>, R> {

    private MapPartitionFunction<T, R> fun;

    public MapPartitionFunctionImpl(MapPartitionFunction<T, R> function) {
        fun = function;
    }

    public Iterator<R> call(Iterator<T> tIterator) throws Exception {
        return (Iterator<R>) fun.mapPartition(Utils.iterable(tIterator));
    }
}