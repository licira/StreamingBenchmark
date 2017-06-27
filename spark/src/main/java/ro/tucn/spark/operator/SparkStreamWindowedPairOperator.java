package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.operator.StreamWindowedPairOperator;
import ro.tucn.spark.function.PairMapPartitionFunctionImpl;
import ro.tucn.spark.function.ReduceFunctionImpl;
import ro.tucn.spark.function.UpdateStateFunctionImpl;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkStreamWindowedPairOperator<K, V> extends StreamWindowedPairOperator<K, V> {

    private JavaPairDStream<K, V> pairDStream;

    public SparkStreamWindowedPairOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    @Override
    public StreamPairOperator<K, V> reduceByKey(ReduceFunction<V> fun,
                                                  String componentId) {
        JavaPairDStream<K, V> newStream = this.pairDStream.reduceByKey(new ReduceFunctionImpl(fun));
        return new SparkStreamPairOperator(newStream, parallelism);
    }

    @Override
    public StreamPairOperator<K, V> updateStateByKey(ReduceFunction<V> fun,
                                                       String componentId) {
        JavaPairDStream<K, V> cumulateStream = this.pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        return new SparkStreamPairOperator(cumulateStream, parallelism);
    }

    @Override
    public <R> StreamPairOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.mapPartitionsToPair(new PairMapPartitionFunctionImpl(fun));
        return new SparkStreamPairOperator(newStream, parallelism);
    }

    @Override
    public <R> StreamPairOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                   String componentId) {
        return null;
    }

    @Override
    public StreamPairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun,
                                             String componentId) {
        return null;
    }

    @Override
    public StreamPairOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun,
                                             String componentId) {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Operator not supported");
    }

    @Override
    public void print() {
        pairDStream.print();
    }
}