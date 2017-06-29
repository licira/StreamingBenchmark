package ro.tucn.operator;

/**
 * Created by Liviu on 4/8/2017.
 */
public abstract class StreamWindowedOperator<T> extends BaseOperator {

    public StreamWindowedOperator(int parallelism) {
        super(parallelism);
    }

    // return StreamOperator<R>
    //public abstract <R> StreamOperator<R> mapPartition(MapPartitionFunction<T, R> fun, String componentId);

    // return new StreamOperator<R>();
    //public abstract <R> StreamOperator<R> map(MapFunction<T, R> fun, String componentId);

    // return new StreamOperator<T>();
    //public abstract StreamOperator<T> filter(FilterFunction<T> fun, String componentId);

    // return new StreamOperator<T>();
    //public abstract StreamOperator<T> reduce(ReduceFunction<T> fun, String componentId);

    // return new StreamPairOperator<K,V>
    //public abstract <K, V> StreamPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun, String componentId);

}
