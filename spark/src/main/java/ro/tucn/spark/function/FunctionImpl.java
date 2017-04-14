package ro.tucn.spark.function;

import org.apache.spark.api.java.function.Function;
import ro.tucn.frame.functions.MapFunction;

/**
 * Created by Liviu on 4/8/2017.
 */
public class FunctionImpl<T, R> implements Function<T, R> {

    private MapFunction<T, R> function;

    public FunctionImpl(MapFunction<T, R> fun) {
        function = fun;
    }

    public R call(T t) throws Exception {
        return function.map(t);
    }
}