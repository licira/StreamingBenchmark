package ro.tucn.spark.function.functions;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import ro.tucn.frame.functions.ReduceFunction;

/**
 * Created by Liviu on 4/5/2017.
 */
public class ReduceFunctionImpl<T> implements Function2<T, T, T> {

    private static Logger logger = Logger.getLogger(ReduceFunctionImpl.class);
    private ReduceFunction<T> fun;

    public ReduceFunctionImpl(ReduceFunction<T> function) {
        fun = function;
    }

    public T call(T t1, T t2) throws Exception {
        return fun.reduce(t1, t2);
    }
}
