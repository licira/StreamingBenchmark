package ro.tucn.flink.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.datastream.NoWindowJoinedStreams;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedPairOperator;
import ro.tucn.operator.Operator;
import ro.tucn.statistics.LatencyLog;
import ro.tucn.util.TimeDuration;
import ro.tucn.util.WithTime;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkPairOperator<K, V> extends PairOperator<K, V> {

    private DataStream<Tuple2<K, V>> dataStream;

    public FlinkPairOperator(DataStream<Tuple2<K, V>> dataStream, int parallelism) {
        super(parallelism);
        this.dataStream = dataStream;
    }

    public FlinkGroupedOperator<K, V> groupByKey() {
        KeyedStream<Tuple2<K, V>, Object> keyedStream = dataStream.keyBy(
                (KeySelector<Tuple2<K, V>, Object>) value -> value._1()
        );
        return new FlinkGroupedOperator<>(keyedStream, parallelism);
    }

    // TODO: reduceByKey - reduce first then groupByKey, at last reduce again
    @Override
    public PairOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.keyBy((KeySelector<Tuple2<K, V>, Object>) value -> value._1())
                .reduce((org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2)
                        -> new Tuple2<>(t1._1(), fun.reduce(t1._2(), t2._2())));
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> mapValue(final MapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K, R>> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, Tuple2<K, R>>) tuple2
                -> new Tuple2<>(tuple2._1(), fun.map(tuple2._2())));
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> Operator<R> map(final MapFunction<Tuple2<K, V>, R> fun, final String componentId) {
        DataStream<R> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R>) value ->
                fun.map(value));
        return new FlinkOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> Operator<R> map(final MapFunction<Tuple2<K, V>, R> fun, final String componentId, Class<R> outputClass) {
        org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R> mapper
                = (org.apache.flink.api.common.functions.MapFunction<Tuple2<K, V>, R>) value -> fun.map(value);
        TypeInformation<R> outType = TypeExtractor.getForClass(outputClass);
        DataStream<R> newDataStream = dataStream.transform("Map", outType,
                new StreamMap<>(dataStream.getExecutionEnvironment().clean(mapper)));
        return new FlinkOperator<>(newDataStream, parallelism);
    }

    @Override
    public <R> PairOperator<K, R> flatMapValue(final FlatMapFunction<V, R> fun, String componentId) {
        DataStream<Tuple2<K, R>> newDataStream = dataStream.flatMap((org.apache.flink.api.common.functions.FlatMapFunction<Tuple2<K, V>, Tuple2<K, R>>) (tuple2, collector) -> {
            Iterable<R> rIterable = (Iterable<R>) fun.flatMap(tuple2._2());
            for (R r : rIterable)
                collector.collect(new Tuple2<>(tuple2._1(), r));
        });
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public PairOperator<K, V> filter(final FilterFunction<scala.Tuple2<K, V>> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.filter((org.apache.flink.api.common.functions.FilterFunction<Tuple2<K, V>>) kvTuple2 ->
                fun.filter(kvTuple2));
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    /**
     * Whether is windowed stream?
     *
     * @param fun         reduce function
     * @param componentId current component id
     * @return PairOperator
     */
    public PairOperator<K, V> updateStateByKey(ReduceFunction<V> fun, String componentId) {
        return this;
    }


    @Override
    public PairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId, TimeDuration windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration);
    }

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
    @Override
    public PairOperator<K, V> reduceByKeyAndWindow(final ReduceFunction<V> fun, String componentId, TimeDuration windowDuration, TimeDuration slideDuration) {
        org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>> reduceFunction =
                (org.apache.flink.api.common.functions.ReduceFunction<Tuple2<K, V>>) (t1, t2) -> {
                    V result = fun.reduce(t1._2(), t2._2());
                    return new Tuple2<K, V>(t1._1(), result);
                };
        DataStream<Tuple2<K, V>> newDataStream = dataStream
                .keyBy((KeySelector<Tuple2<K, V>, Object>) value -> value._1())
                .timeWindow(Time.of(windowDuration.getLength(),
                        windowDuration.getUnit()),
                        Time.of(slideDuration.getLength(),
                                slideDuration.getUnit()))
                .reduce(reduceFunction);
        return new FlinkPairOperator<>(newDataStream, parallelism);
    }

    @Override
    public WindowedPairOperator<K, V> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    /**
     * Keyed stream window1
     *
     * @param windowDuration window1 size
     * @param slideDuration  slide size
     * @return WindowedPairOperator
     */
    @Override
    public WindowedPairOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        WindowedStream<Tuple2<K, V>, K, TimeWindow> windowedDataStream = dataStream.keyBy((KeySelector<Tuple2<K, V>, K>) tuple2 -> tuple2._1())
                .timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()),
                        Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkWindowedPairOperator<>(windowedDataStream, parallelism);
    }

    /**
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window1 length of this stream
     * @param joinWindowDuration window1 length of joinStream
     * @param <R>                Value type of the other stream
     * @return PairOperator after join
     */
    @Override
    public <R> PairOperator<K, scala.Tuple2<V, R>> join(String componentId,
                                                                PairOperator<K, R> joinStream,
                                                                TimeDuration windowDuration,
                                                                TimeDuration joinWindowDuration) throws WorkloadException {
        return join(componentId, joinStream, windowDuration, joinWindowDuration, null, null);
    }

    /**
     * Event time join
     *
     * @param componentId        current compute component id
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window1 length of this stream
     * @param joinWindowDuration window1 length of joinStream
     * @param eventTimeAssigner1 event time assignment for this stream
     * @param eventTimeAssigner2 event time assignment for joinStream
     * @param <R>                Value type of the other stream
     * @return PairOperator
     * @throws WorkloadException
     */
    @Override
    public <R> PairOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairOperator<K, R> joinStream,
                                                          TimeDuration windowDuration,
                                                          TimeDuration joinWindowDuration,
                                                          final AssignTimeFunction<V> eventTimeAssigner1,
                                                          final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
        if (null != eventTimeAssigner1 && null != eventTimeAssigner2)
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        else
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        if (joinStream instanceof FlinkPairOperator) {
            FlinkPairOperator<K, R> joinFlinkStream = ((FlinkPairOperator<K, R>) joinStream);

            final KeySelector<Tuple2<K, V>, K> keySelector1 = (KeySelector<Tuple2<K, V>, K>) kvTuple2 -> kvTuple2._1();
            final KeySelector<Tuple2<K, R>, K> keySelector2 = (KeySelector<Tuple2<K, R>, K>) krTuple2 -> krTuple2._1();

            // tmpKeySelector for get keyTypeInfo InvalidTypesException
            // TODO: find a better solution to solve
            KeySelector<K, K> tmpKeySelector = (KeySelector<K, K>) value -> value;
            TypeInformation<K> keyTypeInfo = TypeExtractor.getUnaryOperatorReturnType(tmpKeySelector, KeySelector.class, false, false, dataStream.getType(), null, false);

            DataStream<Tuple2<K, V>> dataStream1 = new KeyedStream<>(dataStream, keySelector1, keyTypeInfo);
            if (null != eventTimeAssigner1) {
                /*final AscendingTimestampExtractor<Tuple2<K, V>> timestampExtractor1 = new AscendingTimestampExtractor<Tuple2<K, V>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<K, V> element, long currentTimestamp) {
                        return eventTimeAssigner1.assign(element._2());
                    }
                };*/
                TimestampExtractor<Tuple2<K, V>> timestampExtractor1 = new TimestampExtractor<Tuple2<K, V>>() {
                    long currentTimestamp = 0L;

                    @Override
                    public long extractTimestamp(Tuple2<K, V> kvTuple2, long l) {
                        long timestamp = eventTimeAssigner1.assign(kvTuple2._2());
                        if (timestamp > currentTimestamp) {
                            currentTimestamp = timestamp;
                        }
                        return timestamp;
                    }

                    @Override
                    public long extractWatermark(Tuple2<K, V> kvTuple2, long l) {
                        return -9223372036854775808L;
                    }

                    @Override
                    public long getCurrentWatermark() {
                        return currentTimestamp - 500;
                    }
                };
                dataStream1 = dataStream1.assignTimestamps(timestampExtractor1);
            }

            DataStream<Tuple2<K, R>> dataStream2 = new KeyedStream<>(joinFlinkStream.dataStream, keySelector2, keyTypeInfo);
            if (null != eventTimeAssigner2) {
                /*final AscendingTimestampExtractor<Tuple2<K, R>> timestampExtractor2 = new AscendingTimestampExtractor<Tuple2<K, R>>() {

                    @Override
                    public long extractAscendingTimestamp(Tuple2<K, R> element, long currentTimestamp) {
                        return eventTimeAssigner2.assign(element._2());
                    }
                };*/
                TimestampExtractor<Tuple2<K, R>> timestampExtractor2 = new TimestampExtractor<Tuple2<K, R>>() {
                    long currentTimestamp = 0L;

                    @Override
                    public long extractTimestamp(Tuple2<K, R> kvTuple2, long l) {
                        long timestamp = eventTimeAssigner2.assign(kvTuple2._2());
                        if (timestamp > currentTimestamp) {
                            currentTimestamp = timestamp;
                        }
                        return timestamp;
                    }

                    @Override
                    public long extractWatermark(Tuple2<K, R> kvTuple2, long l) {
                        return -9223372036854775808L;
                    }

                    @Override
                    public long getCurrentWatermark() {
                        return currentTimestamp - 500;
                    }
                };
                dataStream2 = dataStream2.assignTimestamps(timestampExtractor2);
            }

            DataStream<Tuple2<K, Tuple2<V, R>>> joineStream =
                    new NoWindowJoinedStreams<>(dataStream1, dataStream2)
                            .where(keySelector1, keyTypeInfo)
                            .buffer(Time.of(windowDuration.toMilliSeconds(), TimeUnit.MILLISECONDS))
                            .equalTo(keySelector2, keyTypeInfo)
                            .buffer(Time.of(joinWindowDuration.toMilliSeconds(), TimeUnit.MILLISECONDS))
                            .apply((JoinFunction<Tuple2<K, V>, Tuple2<K, R>, Tuple2<K, Tuple2<V, R>>>) (first, second) -> new Tuple2<>(first._1(), new Tuple2<>(first._2(), second._2())));

            return new FlinkPairOperator<>(joineStream, parallelism);
        } else {
            throw new WorkloadException("Cast joinStrem to FlinkPairOperator failed");
        }
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        dataStream.print();
    }

    @Override
    public void sink() {
        //dataStream.print();
        dataStream.addSink(new SinkFunction<Tuple2<K, V>>() {
            LatencyLog latency = new LatencyLog("sink");

            @Override
            public void invoke(Tuple2<K, V> value) throws Exception {
                latency.execute((WithTime<? extends Object>) value._2());
            }
        });
    }

    @Override
    public void count() {

    }
}
