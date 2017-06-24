package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.*;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.Operator;
import ro.tucn.operator.PairOperator;
import ro.tucn.operator.WindowedOperator;
import ro.tucn.spark.function.*;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
    public PairOperator<String, Integer> wordCount() {
        JavaDStream<String> stringJavaDStream = dStream.flatMap(x -> Arrays.asList(((String)x).split(" ")).iterator());
        JavaPairDStream<String, Integer> tIntegerJavaPairDStream = stringJavaDStream.mapToPair(s -> new Tuple2(s, 1));
        JavaPairDStream<String, Integer> tIntegerJavaPairDStream1 = tIntegerJavaPairDStream.reduceByKey((i1, i2) -> i1 + i2);
        return new SparkPairOperator(tIntegerJavaPairDStream1, parallelism);
    }

    @Override
    public void kMeansCluster(Operator<T> centroidsOperator) throws WorkloadException {
        checkOperatorType(centroidsOperator);

        JavaDStream<Point> points = (JavaDStream<Point>) this.dStream;
        JavaDStream<Point> centroids = (JavaDStream<Point>) ((SparkOperator<Point>) centroidsOperator).dStream;



    }

    private class MapFunctionn<T, R> implements Function<T, T>, Serializable {

        JavaDStream<List<T>> glom;

        @Override
        public T call(T t) throws Exception {

            return t;
        }

        public void setGlom(JavaDStream<List<T>> glom) {
            this.glom = glom;
        }
    }

    private class NearestCenterSelector<T, K, V> implements PairFunction<T, K, V> {
        private List<Point> centroidsList;
        private SerializableJavaDStream<Point> centroids;

        @Override
        public Tuple2<K, V> call(T t) throws Exception {
            return null;
        }

        public void setCentroids(SerializableJavaDStream<Point> centroids) {
            this.centroids = centroids;
            centroids.mapToPair(point -> {
                centroidsList.add(point);
                return null;
            });
        }

        public void setCentroidsList(List<Point> centroidsList) {
            this.centroidsList = centroidsList;
        }
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
    public PairOperator mapToPair(Operator<T> centroids) {
        return null;
    }

    @Override
    public Operator map(Operator<T> points) {
        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {
        //throw new UnsupportOperatorException("Operator not supported1");
    }

    @Override
    public void print() {
        /*VoidFunction2<JavaRDD<T>, Time> voidFunction2 = (VoidFunction2<JavaRDD<T>, Time>) (rdd, time) -> {
            rdd.collect();
            logger.info("===================================");
            logger.info(" Number of records in this batch: " + rdd.count());
            logger.info("===================================");
        };*/
        //this.dStream.foreachRDD(voidFunction2);
        this.dStream.print();
    }

    private void checkOperatorType(Operator<T> centroids) throws WorkloadException {
        if (!(centroids instanceof SparkOperator)) {
            throw new WorkloadException("Cast joinStream to SparkPairOperator failed");
        }
    }



    public static final class CountAppender implements Function<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> call(Tuple2<Integer, Point> t) throws Exception {
            return new Tuple3<>(t._1, t._2, 1L);
        }
    }
}
