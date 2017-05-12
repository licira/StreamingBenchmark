package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.frame.functions.*;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.Operator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedOperator;
import ro.tucn.spark.function.*;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkOperator<T> extends Operator<T> {

    private static final Logger logger = Logger.getLogger(SparkOperator.class);

    JavaDStream<T> dStream;

    public SparkOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;
    }

    @Override
    public <R> Operator<R> map(final MapFunction<T, R> fun,
                                       String componentId) {
        JavaDStream<R> newStream = dStream.map(new FunctionImpl(fun));
        return new SparkOperator<R>(newStream, parallelism);
    }

    @Override
    public <R> Operator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Operator not supported");
    }

    @Override
    public <R> Operator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException {
        throw new UnsupportOperatorException("Operator not supported");
    }

    @Override
    public <K, V> PairOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.mapToPair(new PairFunctionImpl(fun));
        return new SparkPairOperator(pairDStream, parallelism);
    }

    @Override
    public Operator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.reduce(new ReduceFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public Operator<T> filter(final FilterFunction<T> fun, String componentId) {
        JavaDStream<T> newStream = dStream.filter(new FilterFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public <R> Operator<R> flatMap(final FlatMapFunction<T, R> fun,
                                           String componentId) {
        JavaDStream<R> newStream = dStream.flatMap(new FlatMapFunctionImpl(fun));
        return new SparkOperator(newStream, parallelism);
    }

    @Override
    public <K, V> PairOperator<K, V> flatMapToPair(final FlatMapPairFunction<T, K, V> fun,
                                                           String componentId) {
        JavaPairDStream<K, V> pairDStream = dStream.flatMapToPair(new PairFlatMapFunction<T, K, V>() {
            @Override
            public Iterator<Tuple2<K, V>> call(T t) throws Exception {
                return (Iterator<Tuple2<K, V>>) fun.flatMapToPair(t);
            }
        });
        return new SparkPairOperator<>(pairDStream, parallelism);
    }

    @Override
    public <K, V> PairOperator<K, V> flatMapToPair(FlatMapPairFunction<T, K, V> fun) {
        JavaPairDStream<K, V> pairDStream = dStream.flatMapToPair(new PairFlatMapFunction<T, K, V>() {
            @Override
            public Iterator<Tuple2<K, V>> call(T t) throws Exception {
                return (Iterator<Tuple2<K, V>>) fun.flatMapToPair(t);
            }
        });
        return new SparkPairOperator<>(pairDStream, parallelism);
    }

    @Override
    public WindowedOperator<T> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public WindowedOperator<T> window(TimeDuration windowDuration,
                                              TimeDuration slideDuration) {
        Duration windowDurations = ro.tucn.spark.util.Utils.timeDurationsToSparkDuration(windowDuration);
        Duration slideDurations = ro.tucn.spark.util.Utils.timeDurationsToSparkDuration(slideDuration);

        JavaDStream<T> windowedStream = dStream.window(windowDurations, slideDurations);
        return new SparkWindowedOperator(windowedStream, parallelism);
    }

    @Override
    public void sink() {

    }

    @Override
    public PairOperator<String, Integer> flatMapToPair() {
        Pattern SPACE = Pattern.compile(" ");
        JavaDStream<String> stringJavaDStream = dStream.flatMap(x -> Arrays.asList(((String)x).split(" ")).iterator());
        stringJavaDStream.print();
        JavaPairDStream<String, Integer> tIntegerJavaPairDStream = stringJavaDStream.mapToPair(s -> new Tuple2(s, 1));
        tIntegerJavaPairDStream.print();
        JavaPairDStream<String, Integer> tIntegerJavaPairDStream1 = tIntegerJavaPairDStream.reduceByKey((i1, i2) -> i1 + i2);
        tIntegerJavaPairDStream1.print();
        return new SparkPairOperator(tIntegerJavaPairDStream1, parallelism);
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        //throw new UnsupportOperatorException("Operator not supported1");
    }

    @Override
    public void print() {
        VoidFunction2<JavaRDD<T>, Time> voidFunction2 = (VoidFunction2<JavaRDD<T>, Time>) (rdd, time) -> {
            rdd.collect();
            logger.info("===================================");
            logger.info(" Number of records in this batch: " + rdd.count());
            logger.info("===================================");
        };
        //this.dStream.foreachRDD(voidFunction2);
        this.dStream.print();
    }
}
