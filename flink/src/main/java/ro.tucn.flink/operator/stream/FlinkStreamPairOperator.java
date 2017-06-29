package ro.tucn.flink.operator.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.datastream.NoWindowJoinedStreams;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;

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

    private <R> void checkOperatorType(PairOperator<K, R> joinStream) throws WorkloadException {
        if (!(joinStream instanceof FlinkStreamPairOperator)) {
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
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
}
