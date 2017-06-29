package ro.tucn.flink.operator.stream;

import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.StreamWindowedOperator;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkStreamWindowedOperator<T, W extends Window> extends StreamWindowedOperator<T> {

    private WindowedStream<T, T, W> windowStream;

    public FlinkStreamWindowedOperator(WindowedStream<T, T, W> stream, int parallelism) {
        super(parallelism);
        windowStream = stream;
    }

    /*@Override
    public <R> StreamOperator<R> mapPartition(final MapPartitionFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = windowStream.apply((WindowFunction<T, R, T, W>) (t, window, values, collector) -> {
            Iterable<R> results = fun.mapPartition(values);
            for (R r : results) {
                collector.collect(r);
            }
        });
        return new FlinkStreamOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> StreamOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = windowStream.apply((WindowFunction<T, R, T, W>) (t, window, values, collector) -> {
            for (T value : values) {
                R result = fun.map(value);
                collector.collect(result);
            }
        });
        return new FlinkStreamOperator<>(newDataStream, parallelism);
    }

    @Override
    public StreamOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = windowStream.apply((WindowFunction<T, T, T, W>) (t, window, values, collector) -> {
            for (T value : values) {
                if (fun.filter(value))
                    collector.collect(value);
            }
        });
        return new FlinkStreamOperator<>(newDataStream, parallelism);
    }

    @Override
    public StreamOperator<T> reduce(ro.tucn.frame.functions.ReduceFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = windowStream.reduce((ReduceFunction<T>) (t, t1) ->
                fun.reduce(t, t1));
        return new FlinkStreamOperator<>(newDataStream, parallelism);
    }

    @Override
    public <K, V> StreamPairOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = windowStream.apply((WindowFunction<T, Tuple2<K, V>, T, W>) (t, window, values, collector) -> {
            for (T value : values) {
                Tuple2<K, V> result = fun.mapToPair(value);
                collector.collect(result);
            }
        });
        return new FlinkStreamPairOperator<>(newDataStream, parallelism);
    }*/

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}