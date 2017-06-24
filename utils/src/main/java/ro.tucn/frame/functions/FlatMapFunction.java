package ro.tucn.frame.functions;

import java.util.Iterator;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface FlatMapFunction<T, R> extends SerializableFunction {

    Iterator<R> flatMap(T var1) throws Exception;
}