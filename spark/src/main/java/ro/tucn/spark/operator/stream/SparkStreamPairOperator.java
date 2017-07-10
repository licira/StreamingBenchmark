package ro.tucn.spark.operator.stream;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.operator.StreamWindowedPairOperator;
import ro.tucn.spark.statistics.PerformanceStreamingListener;
import ro.tucn.spark.util.Utils;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkStreamPairOperator<K, V> extends StreamPairOperator<K, V> {

    private static final Logger logger = Logger.getLogger(SparkStreamPairOperator.class);

    private JavaPairDStream<K, V> pairDStream;

    public SparkStreamPairOperator(JavaPairDStream<K, V> pairDStream, int parallelism) {
        super(parallelism);
        this.pairDStream = pairDStream;
        frameworkName = "SPARK";
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
    public <R> SparkStreamPairOperator<K, Tuple2<V, R>> advClick(PairOperator<K, R> joinStream,
                                                      TimeDuration windowDuration,
                                                      TimeDuration joinWindowDuration) throws WorkloadException {
        checkWindowDurationsCompatibility(windowDuration, joinWindowDuration);
        checkOperatorType(joinStream);

        Duration duration = toDuration(windowDuration);
        Duration joinDuration = toDuration(joinWindowDuration);

        SparkStreamPairOperator<K, R> joinSparkStream = ((SparkStreamPairOperator<K, R>) joinStream);


        performanceLog.disablePrint();
        performanceLog.setStartTime(TimeHelper.getNanoTime());

        JavaPairDStream<K, Tuple2<V, R>> joinedStream = pairDStream
                .join(joinSparkStream.pairDStream);

        performanceLog.logLatency(TimeHelper.getNanoTime());
        performanceLog.logTotalLatency();
        executionLatency = performanceLog.getTotalLatency();

        PerformanceStreamingListener performanceListener = (PerformanceStreamingListener) this.performanceListener;
        performanceListener.setLatency(executionLatency);

        SparkStreamPairOperator advClickOperator = new SparkStreamPairOperator(joinedStream, parallelism);
        advClickOperator.setExecutionLatency(executionLatency);

        return advClickOperator;
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
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
        }
    }
}