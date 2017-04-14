package ro.tucn.spark.function;

import ro.tucn.frame.functions.FlatMapFunction;

import java.util.Iterator;

/**
 * Created by Liviu on 4/5/2017.
 */
public class FlatMapFunctionImpl<T, R> implements org.apache.spark.api.java.function.FlatMapFunction<T, R> {

    private FlatMapFunction<T, R> fun;

    public FlatMapFunctionImpl(FlatMapFunction<T, R> function) {
        this.fun = function;
    }

    public Iterator<R> call(T t) throws Exception {
        return fun.flatMap(t);
    }
}
