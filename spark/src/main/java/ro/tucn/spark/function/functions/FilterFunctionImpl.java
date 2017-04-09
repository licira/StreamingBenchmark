package ro.tucn.spark.function.functions;

import org.apache.spark.api.java.function.Function;
import ro.tucn.frame.functions.FilterFunction;

/**
 * Created by Liviu on 4/8/2017.
 */
public class FilterFunctionImpl<T> implements Function<T, Boolean> {
    private static final long serialVersionUID = -6809290167835550952L;
    private FilterFunction<T> fun;

    public FilterFunctionImpl(FilterFunction<T> function) {
        fun = function;
    }

    public Boolean call(T t) throws Exception {
        return fun.filter(t);
    }
}
