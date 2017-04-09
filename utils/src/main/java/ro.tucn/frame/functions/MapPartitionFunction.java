package ro.tucn.frame.functions;

/**
 * Created by Liviu on 4/8/2017.
 */
public interface MapPartitionFunction<T, R> {

    Iterable<R> mapPartition(Iterable<T> var1);
}
