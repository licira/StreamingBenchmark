package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedPairOperator;
import ro.tucn.operator.Operator;
import ro.tucn.spark.function.*;
import ro.tucn.spark.util.Utils;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkPairOperator<K, V> extends PairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(SparkPairOperator.class);

    private JavaPairDStream<K, V> pairDStream;

    public SparkPairOperator(JavaPairDStream<K, V> stream, int parallelism) {
        super(parallelism);
        this.pairDStream = stream;
    }

    public SparkGroupedOperator<K, V> groupByKey() {
        JavaPairDStream<K, Iterable<V>> newStream = pairDStream.groupByKey();
        return new SparkGroupedOperator(newStream, parallelism);
    }

    public PairOperator<K, V> reduceByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> newStream = pairDStream.reduceByKey(new ReduceFunctionImpl(fun));
        return new SparkPairOperator(newStream, parallelism);
    }

    public <R> PairOperator<K, R> mapValue(MapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.mapValues(new FunctionImpl(fun));
        return new SparkPairOperator(newStream, parallelism);
    }

    public <R> Operator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId) {
        return null;
    }

    public <R> Operator<R> map(MapFunction<Tuple2<K, V>, R> fun, String componentId, Class<R> outputClass) {
        return null;
    }

    public <R> PairOperator<K, R> flatMapValue(FlatMapFunction<V, R> fun, String componentId) {
        JavaPairDStream<K, R> newStream = pairDStream.flatMapValues(new FlatMapValuesFunctionImpl(fun));
        return new SparkPairOperator(newStream, parallelism);
    }

    public PairOperator<K, V> filter(FilterFunction<Tuple2<K, V>> fun, String componentId) {
        JavaPairDStream<K, V> newStream = pairDStream.filter(new FilterFunctionImpl(fun));
        return new SparkPairOperator(newStream, parallelism);
    }

    public PairOperator<K, V> updateStateByKey(final ReduceFunction<V> fun, String componentId) {
        JavaPairDStream<K, V> cumulateStream = pairDStream.updateStateByKey(new UpdateStateFunctionImpl(fun));
        //cumulateStream.checkpoint(new Duration(60000));
        return new SparkPairOperator(cumulateStream, parallelism);
    }

    public PairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDuration windowDuration) {
        return reduceByKeyAndWindow(fun, componentId, windowDuration, windowDuration);
    }

    public PairOperator<K, V> reduceByKeyAndWindow(ReduceFunction<V> fun, String componentId,
                                                           TimeDuration windowDuration, TimeDuration slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> accumulateStream = pairDStream.reduceByKeyAndWindow(new ReduceFunctionImpl<V>(fun), windowDurations, slideDurations);
        return new SparkPairOperator(accumulateStream, parallelism);
    }

    public WindowedPairOperator<K, V> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    public WindowedPairOperator<K, V> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = Utils.timeDurationsToSparkDuration(slideDuration);
        JavaPairDStream<K, V> windowedStream = pairDStream.window(windowDurations, slideDurations);
        return new SparkWindowedPairOperator(windowedStream, parallelism);
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
    public <R> PairOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairOperator<K, R> joinStream,
                                                          TimeDuration windowDuration,
                                                          TimeDuration joinWindowDuration) throws WorkloadException {
        if (windowDuration.toMilliSeconds() % windowDuration.toMilliSeconds() != 0) {
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }

        Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(joinWindowDuration);

        if (joinStream instanceof SparkPairOperator) {
            SparkPairOperator<K, R> joinSparkStream = ((SparkPairOperator<K, R>) joinStream);
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2, windowDurations2));
            // filter illegal joined data

            return new SparkPairOperator(joinedStream, parallelism);
        }
        throw new WorkloadException("Cast joinStream to SparkPairOperator failed");
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
    public <R> PairOperator<K, Tuple2<V, R>> join(String componentId,
                                                          PairOperator<K, R> joinStream,
                                                          TimeDuration windowDuration,
                                                          TimeDuration joinWindowDuration,
                                                          final AssignTimeFunction<V> eventTimeAssigner1,
                                                          final AssignTimeFunction<R> eventTimeAssigner2) throws WorkloadException {
        if (windowDuration.toMilliSeconds() % windowDuration.toMilliSeconds() != 0) {
            throw new WorkloadException("WindowDuration should be multi times of joinWindowDuration");
        }

        final Duration windowDurations = Utils.timeDurationsToSparkDuration(windowDuration);
        Duration windowDurations2 = Utils.timeDurationsToSparkDuration(joinWindowDuration);

        if (joinStream instanceof SparkPairOperator) {
            SparkPairOperator<K, R> joinSparkStream = ((SparkPairOperator<K, R>) joinStream);
            JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                    .window(windowDurations.plus(windowDurations2), windowDurations2)
                    .join(joinSparkStream.pairDStream.window(windowDurations2, windowDurations2));
            //filter illegal joined data
            //joinedStream.filter(filterFun);
            return new SparkPairOperator(joinedStream, parallelism);
        }
        throw new WorkloadException("Cast joinStream to SparkPairOperator failed");
    }

    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Not implemented yet");
    }

    public void print() {
        VoidFunction2<JavaPairRDD<K, V>, Time> voidFunction2 = (VoidFunction2<JavaPairRDD<K, V>, Time>) (rdd, time) -> {
            rdd.collect();
            logger.info("===================================");
            logger.info(" Number of records in this batch: " + rdd.count());
            logger.info("===================================");
        };
        this.pairDStream.foreachRDD(voidFunction2);
        this.pairDStream.print();
    }

    public void sink() {
        this.pairDStream = this.pairDStream.filter(new PairLatencySinkFunction<K, V>());
        this.pairDStream.count().print();
    }
}