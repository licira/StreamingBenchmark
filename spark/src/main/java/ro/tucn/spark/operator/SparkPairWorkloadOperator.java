package ro.tucn.spark.operator;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairWorkloadOperator;
import ro.tucn.operator.WindowedPairWorkloadOperator;
import ro.tucn.operator.WorkloadOperator;
import ro.tucn.spark.function.*;
import ro.tucn.spark.util.Utils;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkPairWorkloadOperator<K, V> extends PairWorkloadOperator<K, V> {

    private JavaPairDStream<K, V> pairDStream;

    public SparkPairWorkloadOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    public SparkGroupedWorkloadOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkGroupedWorkloadOperator(newStream, parallelism);
    }

    public PairWorkloadOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> newStream = pairDStream.reduceByKey(new ReduceFunctionImpl(fun));
        return new SparkPairWorkloadOperator(newStream, parallelism);
    }

    public <R> PairWorkloadOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.mapValues(new FunctionImpl(fun));
        return new SparkPairWorkloadOperator(newStream, parallelism);
    }

    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId) {
        return null;
    }

    public <R> WorkloadOperator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass) {
        return null;
    }

    public <R> PairWorkloadOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.flatMapValues(new FlatMapValuesFunctionImpl(fun));
        return new SparkPairWorkloadOperator(newStream, parallelism);
    }

    public PairWorkloadOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        JavaPairDStream<K, V> newStream = pairDStream.filter(new FilterFunctionImpl(fun));
        return new SparkPairWorkloadOperator(newStream, parallelism);
    }

    public PairWorkloadOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulateStream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        //cumulateStream.checkpoint(new Duration(60000));
        return new SparkPairWorkloadOperator(cumulateStream, parallelism);
    }

    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDuration windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    public PairWorkloadOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDuration windowDuration, TimeDuration slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> accumulateStream = pairDStream.reduceByKeyAndWindow(new ReduceFunctionImpl<V>(fun), windowDurations, slideDurations);
        return new SparkPairWorkloadOperator(accumulateStream, parallelism);
    }

    public WindowedPairWorkloadOperator<K, V> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    public WindowedPairWorkloadOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> windowedStream = pairDStream.window(windowDurations, slideDurations);
        return new SparkWindowedPairWorkloadOperator(windowedStream, parallelism);
    }

    /**
     * Join two streams base on processing time
     *
     * @param componentId
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param joinWindowDuration window length of joinStream
     * @param <R>
     * @return
     * @throws WorkloadException
     */
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairWorkloadOperator<K, R> joinStream,
                                                          TimeDuration windowDuration,
                                                          TimeDuration joinWindowDuration) throws WorkloadException {
        if (windowDuration.toMilliSeconds() % windowDuration.toMilliSeconds() != 0) {
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }

        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(joinWindowDuration);

        if (joinStream instanceof SparkPairWorkloadOperator) {
            SparkPairWorkloadOperator<K, R> joinSparkStream = ((SparkPairWorkloadOperator<K, R>) joinStream);
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2, windowDurations2));
            // filter illegal joined data

            return new SparkPairWorkloadOperator(joinedStream, parallelism);
        }
        throw new WorkloadException("Cast joinStream to SparkPairWorkloadOperator failed");
    }

    /**
     * Spark doesn't support event time join yet
     *
     * @param componentId
     * @param joinStream         the other stream<K,R>
     * @param windowDuration     window length of this stream
     * @param joinWindowDuration window length of joinStream
     * @param eventTimeAssigner1 event time assignment for this stream
     * @param eventTimeAssigner2 event time assignment for joinStream
     * @param <R>
     * @return
     * @throws WorkloadException
     */
    public <R> PairWorkloadOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairWorkloadOperator<K, R> joinStream,
                                                          TimeDuration windowDuration,
                                                          TimeDuration joinWindowDuration,
                                                          final AssignTimeFunction<V> eventTimeAssigner1,
                                                          final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        if (windowDuration.toMilliSeconds() % windowDuration.toMilliSeconds() != 0) {
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }

        final Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(joinWindowDuration);

        if (joinStream instanceof SparkPairWorkloadOperator) {
            SparkPairWorkloadOperator<K, R> joinSparkStream = ((SparkPairWorkloadOperator<K, R>) joinStream);
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2, windowDurations2));
            //filter illegal joined data
            //joinedStream.filter(filterFun);
            return new SparkPairWorkloadOperator(joinedStream, parallelism);
        }
        throw new WorkloadException("Cast joinStream to SparkPairWorkloadOperator failed");
    }

    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Not implemented yet");
    }

    public void print() {
        this.pairDStream.print();
    }

    public void sink() {
        this.pairDStream = this.pairDStream.filter(new PairLatencySinkFunction<K, V>());
        this.pairDStream.count().print();
    }
}