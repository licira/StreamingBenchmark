package ro.tucn.frame.functions;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface MapWithInitListFunction<T, R> extends SerializableFunction {

    R map(T var1, List<T> list);
}
