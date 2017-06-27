package ro.tucn.spark.operator;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.GroupedOperator;
import ro.tucn.operator.StreamOperator;
import ro.tucn.spark.function.GrouperPairFunctionImpl;

/**
 * Created by Liviu on 4/5/2017.
 */
public class SparkGroupedOperator<K, V> extends GroupedOperator<K, V> {

    private JavaPairDStream<K, Iterable<V>> pairDStream;

    public SparkGroupedOperator(JavaPairDStream<K, Iterable<V>> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    public SparkStreamPairOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId, int parallelism) {
        JavaPairDStream<K, V> newStream = this.pairDStream.mapToPair(new GrouperPairFunctionImpl<K, V>(fun));
        return new SparkStreamPairOperator<K, V>(newStream, parallelism);
    }

    @Override
    public StreamOperator aggregateReduceByKey() {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Not implemented yet");
    }

    @Override
    public void print() {
    }
}