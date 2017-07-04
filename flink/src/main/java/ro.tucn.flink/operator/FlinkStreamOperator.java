package ro.tucn.flink.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.flink.operator.stream.FlinkStreamPairOperator;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.Operator;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.StreamPairOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;

/**
 * Created by Liviu on 4/17/2017.
 */
public class FlinkStreamOperator<T> extends StreamOperator<T> {

    private DataStream<T> dataStream;
    private Logger logger = Logger.getLogger(FlinkStreamOperator.class.getSimpleName());

    public FlinkStreamOperator(DataStream<T> dataStream, int parallelism) {
        super(parallelism);
        this.dataStream = dataStream;
    }

    @Override
    public StreamPairOperator<String, Integer> wordCount() {
        DataStream<String> sentence = dataStream.map(new MapFunction<T, String>() {
            @Override
            public String map(T t) throws Exception {
                return (String) t;

            }
        });
        DataStream<Tuple2<String, Integer>> wordsCountedByOneEach = sentence.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : sentence.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedWords = wordsCountedByOneEach.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(org.apache.flink.api.java.tuple.Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        DataStream<Tuple2<String, Integer>> countedWords = keyedWords.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
        return new FlinkStreamPairOperator<String, Integer>(countedWords, parallelism);
    }

    @Override
    public void kMeansCluster(Operator centroidsOperator) throws WorkloadException {
        checkOperatorType(centroidsOperator);

        DataStream<Point> points = (DataStream<Point>) this.dataStream;
        DataStream<Point> centroids = ((FlinkStreamOperator<Point>) centroidsOperator).dataStream;

        NearestCenterSelector nearestCenterSelector = new NearestCenterSelector();
        for (int i = 0; i < 10; i++) {
            DataStream<Tuple2<Long, Point>> pointsWithCentroid = centroids.connect(points).flatMap(nearestCenterSelector);
            pointsWithCentroid.print();
            centroids = pointsWithCentroid.connect(pointsWithCentroid).flatMap(new CoFlatMapFunction<Tuple2<Long, Point>, Tuple2<Long, Point>, Point>() {

                private Map<Long, Tuple2<Point, Long>> centroidsWithCummulatedCoordinates = new HashMap<Long, Tuple2<Point, Long>>();

                @Override
                public void flatMap1(Tuple2<Long, Point> t, Collector<Point> collector) throws Exception {
                    if (centroidsWithCummulatedCoordinates.containsKey(t.f0)) {
                        Tuple2<Point, Long> centroidWithCummulatedCoordinatesAndFrequencyTuple = centroidsWithCummulatedCoordinates.get(t.f0);
                        centroidsWithCummulatedCoordinates.remove(t.f0);
                        Point centroidWithCummulatedCoordinates = centroidWithCummulatedCoordinatesAndFrequencyTuple.f0;
                        Long frequency = centroidWithCummulatedCoordinatesAndFrequencyTuple.f1;
                        Point newCentroidWithCummulatedCoordinates = centroidWithCummulatedCoordinates.add(t.f1);
                        frequency += 1;
                        Tuple2<Point, Long> newCentroidWithCummulatedCoordinatesAndFrequencyTuple = new Tuple2<Point, Long>(newCentroidWithCummulatedCoordinates, frequency);
                        centroidsWithCummulatedCoordinates.put(t.f0, newCentroidWithCummulatedCoordinatesAndFrequencyTuple);
                    } else {
                        Point newCentroid = new Point(t.f0, t.f1.getCoordinates());
                        Long frequency = 1L;
                        Tuple2<Point, Long> newCentroidWithCummulatedCoordinatesAndFrequencyTuple = new Tuple2<Point, Long>(newCentroid, frequency);
                        centroidsWithCummulatedCoordinates.put(t.f0, newCentroidWithCummulatedCoordinatesAndFrequencyTuple);
                    }
                }

                @Override
                public void flatMap2(Tuple2<Long, Point> t, Collector<Point> collector) throws Exception {
                    if (centroidsWithCummulatedCoordinates.containsKey(t.f0)) {
                        Tuple2<Point, Long> centroidWithCummulatedCoordinatesAndFrequencyTuple = centroidsWithCummulatedCoordinates.get(t.f0);
                        centroidsWithCummulatedCoordinates.remove(t.f0);
                        Point centroidWithCummulatedCoordinates = centroidWithCummulatedCoordinatesAndFrequencyTuple.f0;
                        Long frequency = centroidWithCummulatedCoordinatesAndFrequencyTuple.f1;
                        Point centroid = centroidWithCummulatedCoordinates.div(frequency);
                        centroid.setId(t.f0);
                        collector.collect(centroid);
                    }
                }
            });
        }
        centroids.print();
    }

    @Override
    public void closeWith(BaseOperator operator, boolean broadcast) throws UnsupportOperatorException {

    }

    public void print() {
        dataStream.print();
    }

    /**
     * @apiNote Workaround to be able to perform flink operations
     */
    private DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> toDataStreamWithFlinkTuple2(DataStream<Tuple2<String, Integer>> dataStreamWithScalaTuple2) {
        return dataStreamWithScalaTuple2.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<String, Integer>, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
            @Override
            public org.apache.flink.api.java.tuple.Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                return new org.apache.flink.api.java.tuple.Tuple2<String, Integer>(tuple2.f0, tuple2.f1);
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

    private void checkOperatorType(Operator<T> centroids) throws WorkloadException {
        if (!(centroids instanceof FlinkStreamOperator)) {
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
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
