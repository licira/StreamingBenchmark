package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedPairOperator;
import ro.tucn.spark.function.PairMapPartitionFunctionImpl;
import ro.tucn.spark.function.ReduceFunctionImpl;
import ro.tucn.spark.function.UpdateStateFunctionImpl;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkWindowedPairOperator<K, V> extends WindowedPairOperator<K, V> {

    private JavaPairDStream<K, V> pairDStream;

    public SparkWindowedPairOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    @Override
    public PairOperator<K, V> reduceByKey(ReduceFunction<V> fun,
                                                  String componentId) {
        JavaPairDStream<K, V> newStream = this.pairDStream.reduceByKey(new ReduceFunctionImpl(fun));
        return new SparkPairOperator(newStream, parallelism);
    }

    @Override
    public PairOperator<K, V> updateStateByKey(ReduceFunction<V> fun,
                                                       String componentId) {
        JavaPairDStream<K, V> cumulateStream = this.pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        return new SparkPairOperator(cumulateStream, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.mapPartitionsToPair(new PairMapPartitionFunctionImpl(fun));
        return new SparkPairOperator(newStream, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                   String componentId) {
        return null;
    }

    @Override
    public PairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun,
                                             String componentId) {
        return null;
    }

    @Override
    public PairOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun,
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