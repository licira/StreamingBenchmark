package ro.tucn.flink.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.function.MapFunctionWithInitList;
import ro.tucn.flink.operator.stream.FlinkStreamPairOperator;
import ro.tucn.flink.operator.stream.FlinkStreamWindowedOperator;
import ro.tucn.frame.functions.*;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.*;
import ro.tucn.util.TimeDuration;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkStreamOperator<T> extends StreamOperator<T> {

    protected DataStream<T> dataStream;
    private Logger logger = Logger.getLogger("the logger");
    private IterativeStream<T> iterativeStream;

    public FlinkStreamOperator(DataStream<T> dataSet, int parallelism) {
        super(parallelism);
        dataStream = dataSet;
    }

    @Override
    public StreamPairOperator<String, Integer> wordCount() {
        logger.info("1");
        DataStream<String> sentenceStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, String>() {
            @Override
            public String map(T t) throws Exception {
                return (String) t;

            }
        });
        logger.info("2");
        DataStream<Tuple2<String, Integer>> wordsStreamScala = sentenceStream.flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : sentence.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> wordsStreamFlink = toDataStreamWithFlinkTuple2(wordsStreamScala);
        logger.info("3");
        KeyedStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>, String> keyedWordsStream = wordsStreamFlink.keyBy(new KeySelector<org.apache.flink.api.java.tuple.Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(org.apache.flink.api.java.tuple.Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        logger.info("4");
        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> wordCountsFlink = keyedWordsStream.reduce(new org.apache.flink.api.common.functions.ReduceFunction<org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
            @Override
            public org.apache.flink.api.java.tuple.Tuple2<String, Integer> reduce(org.apache.flink.api.java.tuple.Tuple2<String, Integer> value1, org.apache.flink.api.java.tuple.Tuple2<String, Integer> value2) throws Exception {
                return new org.apache.flink.api.java.tuple.Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
        logger.info("5");
        DataStream<Tuple2<String, Integer>> wordCountsScala = toDataStreamWithScalaTuple2(wordCountsFlink);
        return new FlinkStreamPairOperator<>(wordCountsScala, parallelism);
    }

    @Override
    public void kMeansCluster(StreamOperator<T> centroidsOperator) throws WorkloadException {
        checkOperatorType(centroidsOperator);

        DataStream<Point> points = (DataStream<Point>) this.dataStream;
        DataStream<Point> centroids = ((FlinkStreamOperator<Point>) centroidsOperator).dataStream;

        NearestCenterSelector nearestCenterSelector = new NearestCenterSelector();
        for (int i = 0; i < 1; i++) {

            DataStream<Tuple2<Long, Point>> pointsWithCentroid = centroids.connect(points).flatMap(nearestCenterSelector);
            pointsWithCentroid.print();

            centroids = pointsWithCentroid.connect(pointsWithCentroid).flatMap(new CoFlatMapFunction<Tuple2<Long, Point>, Tuple2<Long, Point>, Point>() {

                private Map<Long, Tuple2<Point, Long>> centroidsWithCummulatedCoordinates = new HashMap<Long, Tuple2<Point, Long>>();

                @Override
                public void flatMap1(Tuple2<Long, Point> t, Collector<Point> collector) throws Exception {
                    if (centroidsWithCummulatedCoordinates.containsKey(t._1)) {
                        Tuple2<Point, Long> centroidWithCummulatedCoordinatesAndFrequencyTuple = centroidsWithCummulatedCoordinates.get(t._1);
                        centroidsWithCummulatedCoordinates.remove(t._1);
                        Point centroidWithCummulatedCoordinates = centroidWithCummulatedCoordinatesAndFrequencyTuple._1;
                        Long frequency = centroidWithCummulatedCoordinatesAndFrequencyTuple._2;
                        Point newCentroidWithCummulatedCoordinates = centroidWithCummulatedCoordinates.add(t._2);
                        frequency += 1;
                        Tuple2<Point, Long> newCentroidWithCummulatedCoordinatesAndFrequencyTuple = new Tuple2<Point, Long>(newCentroidWithCummulatedCoordinates, frequency);
                        centroidsWithCummulatedCoordinates.put(t._1, newCentroidWithCummulatedCoordinatesAndFrequencyTuple);
                    } else {
                        Point newCentroid = new Point(t._1, t._2.getCoordinates());
                        Long frequency = 1L;
                        Tuple2<Point, Long> newCentroidWithCummulatedCoordinatesAndFrequencyTuple = new Tuple2<Point, Long>(newCentroid, frequency);
                        centroidsWithCummulatedCoordinates.put(t._1, newCentroidWithCummulatedCoordinatesAndFrequencyTuple);
                    }
                }

                @Override
                public void flatMap2(Tuple2<Long, Point> t, Collector<Point> collector) throws Exception {
                    if (centroidsWithCummulatedCoordinates.containsKey(t._1)) {
                        Tuple2<Point, Long> centroidWithCummulatedCoordinatesAndFrequencyTuple = centroidsWithCummulatedCoordinates.get(t._1);
                        centroidsWithCummulatedCoordinates.remove(t._1);
                        Point centroidWithCummulatedCoordinates = centroidWithCummulatedCoordinatesAndFrequencyTuple._1;
                        Long frequency = centroidWithCummulatedCoordinatesAndFrequencyTuple._2;
                        Point centroid = centroidWithCummulatedCoordinates.div(frequency);
                        centroid.setId(t._1);
                        collector.collect(centroid);
                    }
                }
            });
            centroids.print();
        }
    }

    @Override
    public <R> StreamOperator<R> map(final MapFunction<T, R> fun, String componentId) {
        DataStream<R> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<T, R>) t -> fun.map(t));
        return new FlinkStreamOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <R> StreamOperator<R> map(final MapWithInitListFunction<T, R> fun, List<T> initList, String componentId) throws UnsupportOperatorException {
        final MapFunctionWithInitList<T, R> map = new MapFunctionWithInitList<>(fun, initList);
        TypeExtractor.getForClass(Point.class);
        TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(dataStream.getExecutionEnvironment().clean(map), dataStream.getType(),
                Utils.getCallLocationName(), true);

        DataStream<R> newDataStream;
        if (iterativeEnabled) {
            iterativeStream = dataStream.iterate();
            newDataStream =
                    iterativeStream.transform("Map", outType, new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));

        } else {
            newDataStream =
                    dataStream.transform("Map", outType, new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));
        }
        return new FlinkStreamOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <R> StreamOperator<R> map(MapWithInitListFunction<T, R> fun, List<T> initList, String componentId, Class<R> outputClass) throws UnsupportOperatorException {
        final MapFunctionWithInitList<T, R> map = new MapFunctionWithInitList<>(fun, initList);
        TypeInformation<R> outType = TypeExtractor.getForClass(outputClass);
        DataStream<R> newDataStream;
        if (iterativeEnabled) {
            iterativeStream = dataStream.iterate();
            newDataStream =
                    iterativeStream.transform("Map",
                            outType,
                            new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));
        } else {
            newDataStream =
                    dataStream.transform("Map",
                            outType,
                            new FlinkPointAssignMapOperator<>(dataStream.getExecutionEnvironment().clean(map)));
        }
        return new FlinkStreamOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <K, V> StreamPairOperator<K, V> mapToPair(final MapPairFunction<T, K, V> fun, String componentId) {
        DataStream<Tuple2<K, V>> newDataStream = dataStream.map(new org.apache.flink.api.common.functions.MapFunction<T, Tuple2<K, V>>() {
            public Tuple2<K, V> map(T t) throws Exception {
                scala.Tuple2<K, V> tuple2 = fun.mapToPair(t);
                return new Tuple2<>(tuple2._1(), tuple2._2());
            }
        });
        /*
        TypeInformation<Tuple2<K, V>> returnType = TypeExtractor.createTypeInfo(MapFunction.class, fun.getClass(), 1, null, null);
        DataStream<Tuple2<K, V>> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<T, Tuple2<K, V>>) t -> {
            Tuple2<K, V> tuple2 = fun.mapToPair(t);
            return new Tuple2<>(tuple2._1(), tuple2._2());
        }).returns(returnType);
        */
        /*
        TypeInformation<Tuple2<K, V>> returnType = TypeExtractor.createTypeInfo(MapFunction.class, fun.getClass(), 1, null, null);
        DataStream<Tuple2<K, V>> newDataStream = dataStream.map((org.apache.flink.api.common.functions.MapFunction<T, Tuple2<K, V>>) t -> {
            Tuple2<K, V> tuple2 = fun.mapToPair(t);
            return new Tuple2<>(tuple2._1(), tuple2._2());
        }).returns(returnType.getTypeClass());

        newDataStream.print();

        return new FlinkStreamPairOperator<>(newDataStream, getParallelism());
        */
        newDataStream.print();
        return new FlinkStreamPairOperator<K, V>(newDataStream, getParallelism());
    }

    @Override
    public StreamOperator<T> reduce(final ReduceFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = dataStream.keyBy(0).reduce((org.apache.flink.api.common.functions.ReduceFunction<T>) (t, t1) -> fun.reduce(t, t1));
        return new FlinkStreamOperator<>(newDataStream, getParallelism());
    }

    @Override
    public StreamOperator<T> filter(final FilterFunction<T> fun, String componentId) {
        DataStream<T> newDataStream = dataStream.filter((org.apache.flink.api.common.functions.FilterFunction<T>) t ->
                fun.filter(t));
        return new FlinkStreamOperator<>(newDataStream, getParallelism());
    }

    @Override
    public <R> StreamOperator<R> flatMap(final FlatMapFunction<T, R> fun, String componentId) {
        TypeInformation<R> returnType = TypeExtractor.createTypeInfo(FlatMapFunction.class, fun.getClass(), 1, null, null);
        DataStream<R> newDataStream = dataStream.flatMap((org.apache.flink.api.common.functions.FlatMapFunction<T, R>) (t, collector) -> {
            Iterable<R> flatResults = (Iterable<R>) fun.flatMap(t);
            for (R r : flatResults) {
                collector.collect(r);
            }
        }).returns(returnType);
        return new FlinkStreamOperator<>(newDataStream, getParallelism());
    }

    @Override
    public StreamWindowedOperator<T> window(TimeDuration windowDuration) {
        return window(windowDuration, windowDuration);
    }

    @Override
    public StreamWindowedOperator<T> window(TimeDuration windowDuration, TimeDuration slideDuration) {
        WindowedStream<T, T, TimeWindow> windowedStream = dataStream.keyBy((KeySelector<T, T>) value ->
                value).timeWindow(Time.of(windowDuration.getLength(), windowDuration.getUnit()),
                Time.of(slideDuration.getLength(), slideDuration.getUnit()));
        return new FlinkStreamWindowedOperator<>(windowedStream, parallelism);
    }

    @Override
    public void closeWith(BaseOperator operator, boolean broadcast) throws UnsupportOperatorException {
        if (null == iterativeStream) {
            throw new UnsupportOperatorException("iterativeStream could not be null");
        } else if (!operator.getClass().equals(getClass())) {
            throw new UnsupportOperatorException("The close stream should be the same type of the origin stream");
        } else if (!iterativeEnabled) {
            throw new UnsupportOperatorException("Iterative is not enabled.");
        } else {
            FlinkStreamOperator<T> operator_close = (FlinkStreamOperator<T>) operator;
            if (broadcast) {
                iterativeStream.closeWith(operator_close.dataStream.broadcast());
            } else {
                iterativeStream.closeWith(operator_close.dataStream);
            }
        }
        iterativeClosed = true;
    }

    public void print() {
        dataStream.print();
    }

    @Override
    public void sink() {
        /*dataStream.addSink(new org.apache.flink.streaming.api.functions.sink.SinkFunction<T>() {
            private PerformanceLog performanceLog = PerformanceLog.getLogger("sink");

            @Override
            public void invoke(T value) throws Exception {
                performanceLog.logThroughputAndLatencyTimeHolder((TimeHolder<? extends Object>) value);
            }
        });*/
    }

    @Override
    public StreamOperator map(StreamOperator<T> points) {
        return null;
    }

    /**
     * @apiNote Workaround to be able to perform flink operations
     */
    private DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> toDataStreamWithFlinkTuple2(DataStream<Tuple2<String, Integer>> dataStreamWithScalaTuple2) {
        return dataStreamWithScalaTuple2.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<String, Integer>, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
            @Override
            public org.apache.flink.api.java.tuple.Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                return new org.apache.flink.api.java.tuple.Tuple2<String, Integer>(tuple2._1(), tuple2._2());
            }
        });
    }

    /**
     * @apiNote Workaround to be able to perform flink operations
     */
    private DataStream<Tuple2<String, Integer>> toDataStreamWithScalaTuple2(DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> dataStreamWithFlinkTuple2) {
        return dataStreamWithFlinkTuple2.map(new org.apache.flink.api.common.functions.MapFunction<org.apache.flink.api.java.tuple.Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(org.apache.flink.api.java.tuple.Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2.f0, tuple2.f1);
            }
        });
    }

    private void checkOperatorType(StreamOperator<T> centroids) throws WorkloadException {
        if (!(centroids instanceof FlinkStreamOperator)) {
            throw new WorkloadException("Cast joinStream to SparkStreamPairOperator failed");
        }
    }

    private class NearestCenterSelector implements CoFlatMapFunction<Point, Point, Tuple2<Long, Point>> {

        List<Point> centroids = new ArrayList<>();

        @Override
        public void flatMap1(Point point, Collector<Tuple2<Long, Point>> collector) throws Exception {
            centroids.add(point);
        }

        @Override
        public void flatMap2(Point point, Collector<Tuple2<Long, Point>> collector) throws Exception {
            double minDistance = Double.MAX_VALUE;
            Long closestCentroidId = -1L;
            for (Point centroid : centroids) {
                double distance = point.euclideanDistance(centroid);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.getId();
                }
            }
            collector.collect(new Tuple2<Long, Point>(closestCentroidId, point));
        }
    }
}
