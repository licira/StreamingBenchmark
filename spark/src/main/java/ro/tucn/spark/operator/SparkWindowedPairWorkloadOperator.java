package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WindowedPairWorkloadOperator;
import ro.tucn.spark.function.functions.PairMapPartitionFunctionImpl;
import ro.tucn.spark.function.functions.ReduceFunctionImpl;
import ro.tucn.spark.function.functions.UpdateStateFunctionImpl;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkWindowedPairWorkloadOperator<K, V> extends WindowedPairWorkloadOperator<K, V> {

    private JavaPairDStream<K, V> pairDStream;

    public SparkWindowedPairWorkloadOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    @Override
    public PairWorkloadOperator<K, V> reduceByKey(ReduceFunction<V> fun,
                                                  String componentId) {
        JavaPairDStream<K, V> newStream = this.pairDStream.reduceByKey(new ReduceFunctionImpl(fun));
        return new SparkPairWorkloadOperator(newStream, parallelism);
    }

    @Override
    public PairWorkloadOperator<K, V> updateStateByKey(ReduceFunction<V> fun,
                                                       String componentId) {
        JavaPairDStream<K, V> cumulateStream = this.pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        return new SparkPairWorkloadOperator(cumulateStream, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapPartition(MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                       String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.mapPartitionsToPair(new PairMapPartitionFunctionImpl(fun));
        return new SparkPairWorkloadOperator(newStream, parallelism);
    }

    @Override
    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun,
                                                   String componentId) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun,
                                             String componentId) {
        return null;
    }

    @Override
    public PairWorkloadOperator<K, V> reduce(ReduceFunction<Tuple2<K, V>> fun,
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