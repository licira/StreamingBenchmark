package ro.tucn.frame.functions;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface ReduceFunction<T> extends SerializableFunction {

    T reduce(T var1, T var2) throws Exception;
}
