package ro.tucn.frame.functions;

import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface MapPairFunction<T, K, V> extends SerializableFunction {

    Tuple2<K, V> mapToPair(T t);
}