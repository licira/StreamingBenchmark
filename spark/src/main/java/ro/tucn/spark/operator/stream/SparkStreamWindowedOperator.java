package ro.tucn.spark.operator.stream;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.operator.StreamWindowedOperator;
import ro.tucn.operator.StreamOperator;
import ro.tucn.spark.function.*;
import ro.tucn.spark.operator.SparkOperator;

/**
 * Created by Liviu on 4/5/2017.
 */
public class SparkStreamWindowedOperator<T> extends StreamWindowedOperator<T> {

    private JavaDStream<T> dStream;

    public SparkStreamWindowedOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;
    }

    @Override
    public <R> StreamOperator<R> mapPartition(MapPartitionFunction<T, R> fun,
                                                String componentId) {
        JavaDStream<R> newStream = dStream.mapPartitions(new MapPartitionFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public <R> StreamOperator<R> map(MapFunction<T, R> fun,
                                       String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public StreamOperator<T> filter(FilterFunction<T> fun,
                                      String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public StreamOperator<T> reduce(ReduceFunction<T> fun,
                                      String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public <K, V> StreamPairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun,
                                                       String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.mapToPair(new PairFunctionImpl(fun));
        return new SparkStreamPairOperator(pairDStream, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Operator not supported");
    }

    @Override
    public void print() {
        dStream.print();
    }
}