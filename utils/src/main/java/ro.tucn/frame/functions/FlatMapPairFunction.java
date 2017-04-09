package ro.tucn.frame.functions;

import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface FlatMapPairFunction<T, K, V> {
    java.lang.Iterable<Tuple2<K, V>> flatMapToPair(T var1) throws Exception;
}