package ro.tucn.spark.operator.stream;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.*;
import ro.tucn.spark.function.*;
import ro.tucn.spark.operator.SparkGroupedOperator;
import ro.tucn.spark.util.Utils;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkStreamPairOperator<K, V> extends StreamPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(SparkStreamPairOperator.class);

    public JavaPairDStream<K, V> pairDStream;

    public SparkStreamPairOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    /**
     * Join two streams base on processing time
     *
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param joinWindowDuration window length of joinStream
     * @param <R>
     * @return
     * @throws WorkloadException
     */
    @Override
    public <R> PairOperator<K, Tuple2<V, R>> advClick(PairOperator<K, R> joinStream,
                                                  TimeDuration windowDuration,
                                                  TimeDuration joinWindowDuration) throws WorkloadException {
        checkWindowDurationsCompatibility(windowDuration, joinWindowDuration);
        checkOperatorType(joinStream);

        Duration duration = toDuration(windowDuration);
        Duration joinDuration = toDuration(joinWindowDuration);

        SparkStreamPairOperator<K, R> joinSparkStream = ((SparkStreamPairOperator<K, R>) joinStream);
        JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                .window(duration.plus(joinDuration), joinDuration)
                .join(joinSparkStream.pairDStream.window(joinDuration, duration));

        return new SparkStreamPairOperator(joinedStream, parallelism);
    }

    @Override
    public void count() {
        this.pairDStream.count().print();
    }

    public SparkGroupedOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkGroupedOperator(newStream, parallelism);
    }

    public StreamPairOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> newStream = pairDStream.reduceByKey(new ReduceFunctionImpl(fun));
        return new SparkStreamPairOperator(newStream, parallelism);
    }

    public <R> StreamPairOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.mapValues(new FunctionImpl(fun));
        return new SparkStreamPairOperator(newStream, parallelism);
    }

    public <R> StreamOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId) {
        return null;
    }

    public <R> StreamOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass) {
        return null;
    }

    public <R> StreamPairOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.flatMapValues(new FlatMapValuesFunctionImpl(fun));
        return new SparkStreamPairOperator(newStream, parallelism);
    }

    public StreamPairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        JavaPairDStream<K, V> newStream = pairDStream.filter(new FilterFunctionImpl(fun));
        return new SparkStreamPairOperator(newStream, parallelism);
    }

    public StreamPairOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulargestream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        //cumulargestream.checkpoint(new Duration(60000));
        return new SparkStreamPairOperator(cumulargestream, parallelism);
    }

    public StreamPairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                         TimeDuration windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    public StreamPairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun,
                                                         String componentId,
                                                         TimeDuration windowDuration,
                                                         TimeDuration slideWindowDuration) {
        Duration duration = toDuration(windowDuration);
        Duration slideDuration = toDuration(slideWindowDuration);
        JavaPairDStream<K, V> accumulargestream = pairDStream.reduceByKeyAndWindow(new ReduceFunctionImpl<V>(fun), duration, slideDuration);
        return new SparkStreamPairOperator(accumulargestream, parallelism);
    }

    public StreamWindowedPairOperator<K, V> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    public StreamWindowedPairOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideWindowDuration) {
        Duration duration = toDuration(windowDuration);
        Duration slideDuration = toDuration(slideWindowDuration);
        JavaPairDStream<K, V> windowedStream = pairDStream.window(duration, slideDuration);
        return new SparkStreamWindowedPairOperator(windowedStream, parallelism);
    }

    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Not implemented yet");
    }

    public void print() {
        //this.pairDStream.foreachRDD(voidFunction2);
        this.pairDStream.print();
    }

    public void sink() {
        /*this.pairDStream = this.pairDStream.filter(new PairLatencySinkFunction<K, V>());
        this.pairDStream.count().print();*/
    }

    private Duration toDuration(TimeDuration windowDuration) {
        return Utils.timeDurationsToSparkDuration(windowDuration);
    }

    private void checkOperatorType(PairOperator joinStream) throws WorkloadException {
        if (!(joinStream instanceof SparkStreamPairOperator)) {
            throw new WorkloadException("Cast joinStream to SparkStreamPairOperator failed");
        }
    }
}