package ro.tucn.flink.operator.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.datastream.NoWindowJoinedStreams;
import ro.tucn.flink.operator.FlinkGroupedOperator;
import ro.tucn.flink.operator.FlinkStreamOperator;
import ro.tucn.frame.functions.FilterFunction;
import ro.tucn.frame.functions.FlatMapFunction;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.frame.functions.ReduceFunction;
import ro.tucn.operator.*;
import ro.tucn.statistics.LatencyLog;
import ro.tucn.util.TimeDuration;
import ro.tucn.util.TimeHolder;

import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkStreamPairOperator<K, V> extends StreamPairOperator<K, V> {

    private DataStream<Tuple2<K, V>> dataStream;

    public FlinkStreamPairOperator(DataStream<Tuple2<K, V>> dataStream, int parallelism) {
        super(parallelism);
        this.dataStream = dataStream;
    }

    /**
     * @param joinOperator       the other stream<K,R>
     * @param windowDuration     window1 length of this stream
     * @param joinWindowDuration window1 length of joinStream
     * @param <R>                Value type of the other stream
     * @return StreamPairOperator after join
     */
    @Override
    public <R> PairOperator<K, Tuple2<V, R>> advClick(PairOperator<K, R> joinOperator,
                                                  TimeDuration windowDuration,
                                                  TimeDuration joinWindowDuration) throws WorkloadException {
        checkWindowDurationsCompatibility(windowDuration, joinWindowDuration);
        checkOperatorType(joinOperator);

        dataStream.getExecutionEnvironment().setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<? extends Tuple2<K, ?>> stream = ((FlinkStreamPairOperator<K, ? extends Object>) joinOperator).dataStream;

        KeySelector<Tuple2<K, V>, K> keySelector1 = new KeySelector<Tuple2<K, V>, K>() {
            @Override
            public K getKey(Tuple2<K, V> kvTuple2) throws Exception {
                return kvTuple2.f0;
            }
        };
        KeySelector<Tuple2<K, R>, K> keySelector2 = new KeySelector<Tuple2<K, R>, K>() {
            @Override
            public K getKey(Tuple2<K, R> kvTuple2) throws Exception {
                return kvTuple2.f0;
            }
        };

        KeySelector<K, K> tmpKeySelector = new KeySelector<K, K>() {
            @Override
            public K getKey(K k) throws Exception {
                return k;
            }
        };
        TypeInformation<K> keyTypeInfo = TypeExtractor.getUnaryOperatorReturnType(tmpKeySelector, KeySelector.class, false, false, dataStream.getType(), null, false);

        DataStream<Tuple2<K, V>> keyedDataStream = new KeyedStream(stream, keySelector1, keyTypeInfo);
        DataStream<Tuple2<K, R>> joinKeyedDataStream = new KeyedStream(stream, keySelector2, keyTypeInfo);

        DataStream<Tuple2<K, Tuple2<V, R>>> joinedStream =
                new NoWindowJoinedStreams<>(keyedDataStream, joinKeyedDataStream)
                        .where(keySelector1, keyTypeInfo)
                        .buffer(Time.of(windowDuration.toMilliSeconds(), TimeUnit.MILLISECONDS))
                        .equalTo(keySelector2, keyTypeInfo)
                        .buffer(Time.of(joinWindowDuration.toMilliSeconds(), TimeUnit.MILLISECONDS))
                        .apply(new JoinFunction<Tuple2<K, V>, Tuple2<K, R>, Tuple2<K, Tuple2<V, R>>>() {
                            @Override
                            public Tuple2<K, Tuple2<V, R>> join(Tuple2<K, V> kvTuple2, Tuple2<K, R> krTuple2) throws Exception {
                                return new Tuple2<K, Tuple2<V, R>>(kvTuple2.f0, new Tuple2<V, R>(kvTuple2.f1, krTuple2.f1));
                            }
                        });

        return new FlinkStreamPairOperator<>(joinedStream, parallelism);
    }

    /*public FlinkGroupedOperator<K, V> groupByKey() {
        KeyedStream<Tuple2<K, V>, Object> keyedStream = dataStream.keyBy(
                (KeySelector<Tuple2<K, V>, Object>) value -> value.f0
        );
        return new FlinkGroupedOperator<>(keyedStream, parallelism);
    }*/

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    /*@Override
    public StreamPairOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.keyBy((KeySelector<Tuple2<K, V>, Object>) value -> value.f0)
                .reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2)
                        -> new Tuple2<>(t1.f0, fun.reduce(t1.f1, t2.f1)));
        return new FlinkStreamPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> StreamPairOperator<K, R> mapValue(final MapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K, R>> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, Tuple2<K, R>>) tuple2
                -> new Tuple2<>(tuple2.f0, fun.map(tuple2.f1)));
        return new FlinkStreamPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> StreamOperator<R> map(final MapFunction<Tuple2<K, V>, R> fun, final String componentId) {
        DataStream<R> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R>) value ->
                fun.map(value));
        return new FlinkStreamOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> StreamOperator<R> map(final MapFunction<Tuple2<K, V>, R> fun, final String componentId, Class<R> outputClass) {
        org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R> mapper
                = (org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R>) value -> fun.map(value);
        TypeInformation<R> outType = TypeExtractor.getForClass(outputClass);
        DataStream<R> newDataStream = dataStream.transform("Map", outType,
                new StreamMap<>(dataStream.getExecutionEnvironment().clean(mapper)));
        return new FlinkStreamOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> StreamPairOperator<K, R> flatMapValue(final FlatMapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K, R>> newDataStream = dataStream.flatMap((org.apache.flink.api.common.functions.FlatMapFunction<Tuple2<K, V>, Tuple2<K, R>>) (tuple2, collector) -> {
            Iterable<R> rIterable = (Iterable<R>) fun.flatMap(tuple2.f1);
            for (R r : rIterable)
                collector.collect(new Tuple2<>(tuple2.f0, r));
        });
        return new FlinkStreamPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public StreamPairOperator<K, V> filter(final FilterFunction<scala.Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.filter((org.apache.flink.api.common.functions.FilterFunction<Tuple2<K, V>>) kvTuple2 ->
                fun.filter(kvTuple2));
        return new FlinkStreamPairOperator<>(newDataStream, parallelism);
    }*/

    /**
     * Whether is windowed stream?
     *
     * @param fun         reduce function
     * @param componentId current component id
     * @return StreamPairOperator
     */
    public StreamPairOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }


    /*@Override
    public StreamPairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDuration windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration);
    }*/

    // TODO: implement pre-aggregation

    /**
     * Pre-aggregation -> key group -> reduce
     *
     * @param fun            reduce function
     * @param componentId
     * @param windowDuration
     * @param slideDuration
     * @return
     */
    /*@Override
    public StreamPairOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, String componentId, TimeDuration windowDuration, TimeDuration slideDuration) {
        org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>> reduceFunction =
                (org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) -> {
                    V result = fun.reduce(t1.f1, t2.f1);
                    return new Tuple2<K, V>(t1.f0, result);
                };
        DataStream<Tuple2<K, V>> newDataStream = dataStream
                .keyBy((KeySelector<Tuple2<K, V>, Object>) value -> value.f0)
                .timeWindow(Time.of(windowDuration.getLength(),
                        windowDuration.getUnit()),
                        Time.of(slideDuration.getLength(),
                                slideDuration.getUnit()))
                .reduce(reduceFunction);
        return new FlinkStreamPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public StreamWindowedPairOperator<K, V> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }*/

    /**
     * Keyed stream window1
     *
     * @param windowDuration window1 size
     * @param slideDuration  slide size
     * @return StreamWindowedPairOperator
     */
    /*@Override
    public StreamWindowedPairOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        WindowedStream<Tuple2<K, V>, K, TimeWindow> windowedDataStream = dataStream.keyBy((KeySelector<Tuple2<K, V>, K>) tuple2 -> tuple2.f0)
                .timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()),
                        Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkStreamWindowedPairOperator<K, V, K>(windowedDataStream, parallelism);
    }*/

    private <R> void checkOperatorType(PairOperator<K, R> joinStream) throws WorkloadException {
        if (!(joinStream instanceof FlinkStreamPairOperator)) {
            throw new WorkloadException("Cast joinStream to SparkStreamPairOperator failed");
        }
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        dataStream.print();
        System.out.println("7");
    }

    @Override
    public void sink() {
        //dataStream.print();
        dataStream.addSink(new SinkFunction<Tuple2<K, V>>() {
            LatencyLog latency = new LatencyLog("sink");

            @Override
            public void invoke(Tuple2<K, V> value) throws Exception {
                latency.execute((TimeHolder<? extends Object>) value.f1);
            }
        });
    }

    @Override
    public void count() {

    }
}
