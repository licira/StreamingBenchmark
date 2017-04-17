package ro.tucn.flink.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.MapPairFunction;
import ro.tucn.frame.functions.MapPartitionFunction;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WindowedWorkloadOperator;
import ro.tucn.operator.WorkloadOperator;
import scala.Tuple2;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkWindowedWorkloadOperator<T, W extends Window> extends WindowedWorkloadOperator<T> {

    private WindowedStream<T, T, W> windowStream;

    public FlinkWindowedWorkloadOperator(WindowedStream<T, T, W> stream, int parallelism) {
        super(parallelism);
        windowStream = stream;
    }

    @Override
    public <R> WorkloadOperator<R> mapPartition(final MapPartitionFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = this.windowStream.apply(new WindowFunction<T, R, T, W>() {
            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<R> collector) throws Exception {
                Iterable<R> results = fun.mapPartition(values);
                for (R r : results) {
                    collector.collect(r);
                }
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = this.windowStream.apply(new WindowFunction<T, R, T, W>() {
            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<R> collector) throws Exception {
                for (T value : values) {
                    R result = fun.map(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = this.windowStream.apply(new WindowFunction<T, T, T, W>() {
            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<T> collector) throws Exception {
                for (T value : values) {
                    if (fun.filter(value))
                        collector.collect(value);
                }
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(ro.tucn.frame.functions.ReduceFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = this.windowStream.reduce(new ReduceFunction<T>() {
            @Override
            public T reduce(T t, T t1) throws Exception {
                return fun.reduce(t, t1);
            }
        });
        return new FlinkWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = this.windowStream.apply(new WindowFunction<T, Tuple2<K, V>, T, W>() {
            @Override
            public void apply(T t, W window, Iterable<T> values, Collector<Tuple2<K, V>> collector) throws Exception {
                for (T value : values) {
                    Tuple2<K, V> result = fun.mapToPair(value);
                    collector.collect(result);
                }
            }
        });
        return new FlinkPairWorkloadOperator<>(newDataStream, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("not implemented yet");
    }

    @Override
    public void print() {

    }
}