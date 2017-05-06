package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedOperator;
import ro.tucn.operator.Operator;
import ro.tucn.spark.function.*;

/**
 * Created by Liviu on 4/5/2017.
 */
public class SparkWindowedOperator<T> extends WindowedOperator<T> {

    private JavaDStream<T> dStream;

    public SparkWindowedOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;
    }

    @Override
    public <R> Operator<R> mapPartition(MapPartitionFunction<T, R> fun,
                                                String componentId) {
        JavaDStream<R> newStream = dStream.mapPartitions(new MapPartitionFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public <R> Operator<R> map(MapFunction<T, R> fun,
                                       String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public Operator<T> filter(FilterFunction<T> fun,
                                      String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public Operator<T> reduce(ReduceFunction<T> fun,
                                      String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public <K, V> PairOperator<K, V> mapToPair(MapPairFunction<T, K, V> fun,
                                                       String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.mapToPair(new PairFunctionImpl(fun));
        return new SparkPairOperator(pairDStream, parallelism);
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