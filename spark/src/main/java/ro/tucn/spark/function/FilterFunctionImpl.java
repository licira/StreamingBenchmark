package ro.tucn.spark.function;

import org.apache.spark.api.java.function.Function;
import ro.tucn.frame.functions.FilterFunction;

/**
 * Created by Liviu on 4/8/2017.
 */
public class FilterFunctionImpl<T> implements Function<T, Boolean> {

    private static final long serialVersionUID = -6809290167835550952L;
    private FilterFunction<T> function;

    public FilterFunctionImpl(FilterFunction<T> function) {
        this.function = function;
    }

    public Boolean call(T t) throws Exception {
        return function.filter(t);
    }
}
