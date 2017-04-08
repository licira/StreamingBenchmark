package ro.tucn.spark.operator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.operator.WindowedWorkloadOperator;
import ro.tucn.spark.function.functions.*;
import ro.tucn.util.TimeDuration;
import ro.tucn.util.Utils;
import scala.Tuple2;

import java.util.List;

import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WorkloadOperator;

import java.util.List;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkWorkloadOperator<T> extends WorkloadOperator<T> {

    private JavaDStream<T> dStream;

    public SparkWorkloadOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;
    }

    @Override
    public <R> WorkloadOperator<R> map(final MapFunction<T, R> fun,
                                       String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl(fun));
        return new SparkWorkloadOperator<R>(newStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("don't support operator");
    }

    @Override
    public <R> WorkloadOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("don't support operator");
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.mapToPair(new PairFunctionImpl(fun));
        return new SparkPairWorkloadOperator(pairDStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl(fun));
        return new SparkWorkloadOperator(newStream, parallelism);
    }

    @Override
    public WorkloadOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl(fun));
        return new SparkWorkloadOperator(newStream, parallelism);
    }

    @Override
    public <R> WorkloadOperator<R> flatMap(final FlatMapFunction<T, R> fun,
                                           String componentId) {
        JavaDStream<R> newStream = dStream.flatMap(new FlatMapFunctionImpl(fun));
        return new SparkWorkloadOperator(newStream, parallelism);
    }

    @Override
    public <K, V> PairWorkloadOperator<K, V> flatMapToPair(final FlatMapPairFunction<T, K, V> fun,
                                                           String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.flatMapToPair(new PairFlatMapFunction<T, K, V>() {
            public Iterable<Tuple2<K, V>> call(T t) throws Exception {
                return fun.flatMapToPair(t);
            }
        });
        return new SparkPairWorkloadOperator(pairDStream, parallelism);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedWorkloadOperator<T> window(TimeDuration windowDuration,
                                              TimeDuration slideDuration) {
        Duration windowDurations = ro.tucn.spark.util.Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = ro.tucn.spark.util.Utils.timeDurationsToSparkDuration(slideDuration);

        JavaDStream<T> windowedStream = dStream.window(windowDurations, slideDurations);
        return new SparkWindowedWorkloadOperator(windowedStream, parallelism);
    }

    @Override
    public void sink() {

    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("don't support operator");
    }

    @Override
    public void print() {

    }
}
