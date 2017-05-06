package ro.tucn.flink.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedPairOperator;
import scala.Tuple2;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkWindowedPairOperator<K, V, W extends Window> extends WindowedPairOperator<K, V> {

    private WindowedStream<Tuple2<K, V>, K, W> windowStream;

    public FlinkWindowedPairOperator(WindowedStream<Tuple2<K, V>, K, W> stream, int parallelism) {
        super(parallelism);
        windowStream = stream;
    }

    @Override
    public PairOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = windowStream.reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) ->
                new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2())));
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    /**
     * In spark, updateStateByKey is used to accumulate data
     * windowedStream is already keyed in FlinkPairWorkload
     *
     * @param fun         user define funciton
     * @param componentId componment id
     * @return pair workload operator
     */
    @Override
    public PairOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = windowStream.reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) ->
                new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2())));
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> mapPartition(final MapPartitionFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        DataStream<Tuple2<K, R>> newDataStream = windowStream.apply((WindowFunction<Tuple2<K, V>, Tuple2<K, R>, K, W>) (k, window, values, collector) -> {
            Iterable<Tuple2<K, R>> results = fun.mapPartition(values);
            for (Tuple2<K, R> r : results) {
                collector.collect(r);
            }
        });
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> mapValue(final MapFunction<Tuple2<K, V>, Tuple2<K, R>> fun, String componentId) {
        DataStream<Tuple2<K, R>> newDataStream = windowStream.apply((WindowFunction<Tuple2<K, V>, Tuple2<K, R>, K, W>) (k, window, values, collector) -> {
            for (Tuple2<K, V> value : values) {
                Tuple2<K, R> result = fun.map(value);
                collector.collect(result);
            }
        });
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public PairOperator<K, V> filter(final FilterFunction<Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = windowStream.apply((WindowFunction<Tuple2<K, V>, Tuple2<K, V>, K, W>) (k, window, values, collector) -> {
            for (Tuple2<K, V> value : values) {
                if (fun.filter(value))
                    collector.collect(value);
            }
        });
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public PairOperator<K, V> reduce(final ReduceFunction<Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = windowStream.reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) ->
                fun.reduce(t1, t2));
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}