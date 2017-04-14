package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.GroupedWorkloadOperator;
import ro.tucn.spark.function.GrouperPairFunctionImpl;

/**
 * Created by Liviu on 4/5/2017.
 */
public class SparkGroupedWorkloadOperator<K, V> extends GroupedWorkloadOperator<K, V> {

    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkGroupedWorkloadOperator(JavaPairDStream<K, Iterable<V>> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    public SparkPairWorkloadOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId, int parallelism) {
        JavaPairDStream<K, V> newStream = this.pairDStream.mapToPair(new GrouperPairFunctionImpl<K, V>(fun));
        return new SparkPairWorkloadOperator<K, V>(newStream, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Not implemented yet");
    }

    @Override
    public void print() {
    }
}