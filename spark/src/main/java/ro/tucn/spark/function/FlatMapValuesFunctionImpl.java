package ro.tucn.spark.function;

import org.apache.spark.api.java.function.Function;
import ro.tucn.frame.functions.FlatMapFunction;

/**
 * Created by Liviu on 4/5/2017.
 */
public class FlatMapValuesFunctionImpl<V, R> implements Function<V, Iterable<R>> {

    private FlatMapFunction<V, R> function;

    public FlatMapValuesFunctionImpl(FlatMapFunction<V, R> function) {
        this.function = function;
    }

    public Iterable<R> call(V v) throws Exception {
        return (Iterable<R>) function.flatMap(v);
    }
}
