package ro.tucn.frame.functions;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface FilterFunction<T> extends SerializableFunction {

    boolean filter(T var1) throws Exception;
}

