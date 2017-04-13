package ro.tucn.spark.function.functions;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * Created by Liviu on 4/5/2017.
 */
public class FlatMapFunctionImpl<T, R> implements FlatMapFunction<T, R> {

    ro.tucn.frame.functions.FlatMapFunction<T, R> fun;

    public FlatMapFunctionImpl(ro.tucn.frame.functions.FlatMapFunction<T, R> function) {
        this.fun = function;
    }

    public Iterator<R> call(T t) throws Exception {
        return fun.flatMap(t);
    }
}
