package ro.tucn.flink.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.GroupedOperator;
import scala.Tuple2;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkGroupedOperator<K, V> extends GroupedOperator<K, V> {

    private KeyedStream<Tuple2<K, V>, Object> groupedDataStream;

    public FlinkGroupedOperator(KeyedStream<Tuple2<K, V>, Object> groupedDataStream, int parallelism) {
        super(parallelism);
        this.groupedDataStream = groupedDataStream;
    }

    public FlinkPairOperator<K, V> reduce(final ReduceFunction<V> fun, String componentId, int parallelism) {

        DataStream<Tuple2<K, V>> newDataSet = groupedDataStream.reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) ->
                new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2())));
        return new FlinkPairOperator<>(newDataSet, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}
