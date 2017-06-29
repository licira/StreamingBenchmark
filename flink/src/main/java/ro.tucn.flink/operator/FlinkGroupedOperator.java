package ro.tucn.flink.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.flink.operator.stream.FlinkStreamPairOperator;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.GroupedOperator;
import ro.tucn.operator.StreamOperator;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkGroupedOperator<K, V> extends GroupedOperator<K, V> {

    private KeyedStream<Tuple2<K, V>, Object> groupedDataStream;

    public FlinkGroupedOperator(KeyedStream<Tuple2<K, V>, Object> groupedDataStream, int parallelism) {
        super(parallelism);
        this.groupedDataStream = groupedDataStream;
    }

    public FlinkStreamPairOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId, int parallelism) {

        DataStream<Tuple2<K, V>> newDataSet = groupedDataStream.reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) ->
                new Tuple2<>(t1.f0, fun.reduce(t1.f1, t2.f1)));
        return new FlinkStreamPairOperator<K, V>(newDataSet, parallelism);
    }

    @Override
    public StreamOperator aggregateReduceByKey() {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}
