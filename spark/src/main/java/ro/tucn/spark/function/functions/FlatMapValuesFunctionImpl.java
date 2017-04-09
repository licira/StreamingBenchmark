package ro.tucn.spark.function.functions;

import org.apache.spark.api.java.function.Function;
import ro.tucn.frame.functions.FlatMapFunction;

/**
 * Created by Liviu on 4/5/2017.
 */
public class FlatMapValuesFunctionImpl<V, R> implements Function<V, Iterable<R>> {

    FlatMapFunction<V, R> fun;

    public FlatMapValuesFunctionImpl(FlatMapFunction<V, R> function) {
        this.fun = function;
    }

    public Iterable<R> call(V v) throws Exception {
        return fun.flatMap(v);
    }
}
