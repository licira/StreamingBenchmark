package ro.tucn.flink.operator.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.Operator;

import java.util.Collection;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;

/**
 * Created by Liviu on 6/27/2017.
 */
public class FlinkBatchOperator<T> extends BatchOperator<T> {

    private static final Logger logger = Logger.getLogger(FlinkBatchOperator.class);

    private DataSet<T> dataSet;

    public FlinkBatchOperator(DataSet<T> dataSet, int parallelism) {
        super(parallelism);
        this.dataSet = dataSet;
        frameworkName = "FLINK";
    }

    @Override
    public FlinkBatchPairOperator<String, Integer> wordCount() {
        performanceLog.disablePrint();
        performanceLog.setStartTime(TimeHelper.getNanoTime());

        DataSet<Tuple2<String, Integer>> sentences = dataSet.flatMap(new FlatMapFunction<T, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(T t, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String sentence = (String) t;
                for (String word : sentence.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> wordsCountedByOneEach = sentences.groupBy(0);
        DataSet<Tuple2<String, Integer>> countedWords = wordsCountedByOneEach.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        });

        performanceLog.logLatency(TimeHelper.getNanoTime());
        performanceLog.logTotalLatency();
        executionLatency = performanceLog.getTotalLatency();

        return new FlinkBatchPairOperator<String, Integer>(countedWords, parallelism);
    }

    @Override
    public FlinkBatchPairOperator<Point, Integer> kMeansCluster(Operator centroidsOperator) throws WorkloadException {
        checkOperatorType(centroidsOperator);

        DataSet<Point> points = (DataSet<Point>) this.dataSet;
        DataSet<Point> centroids = ((FlinkBatchOperator<Point>) centroidsOperator).dataSet;

        SelectNearestCenter nearestCenterSelector = new SelectNearestCenter();
        CountAppender countAppender = new CountAppender();
        CentroidAccumulator centroidAccumulator = new CentroidAccumulator();
        CentroidAverager centroidAverager = new CentroidAverager();

        performanceLog.disablePrint();
        performanceLog.setStartTime(TimeHelper.getNanoTime());

        IterativeDataSet<Point> loop = centroids.iterate(10);

        DataSet<Point> newCentroids = points
                // compute closest centroid for each point
                .map(nearestCenterSelector).withBroadcastSet(loop, "centroids")
                .map(countAppender)
                .groupBy(0)
                .reduce(centroidAccumulator)
                .map(centroidAverager);

        DataSet<Point> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Long, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(nearestCenterSelector).withBroadcastSet(finalCentroids, "centroids");

        performanceLog.logLatency(TimeHelper.getNanoTime());
        performanceLog.logTotalLatency();
        executionLatency = performanceLog.getTotalLatency();

        centroidsOperator = new FlinkBatchOperator<Point>(finalCentroids, parallelism);
        try {
            finalCentroids.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void closeWith(BaseOperator dataSet, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        try {
            dataSet.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private void checkOperatorType(Operator<T> dataSet) throws WorkloadException {
        if (!(dataSet instanceof FlinkBatchOperator)) {
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
        }
    }

    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Long, Point>> {

        private Collection<Point> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Long, Point> map(Point p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            long closestCentroidId = -1;
            // check all cluster centers
            for (Point centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.getId();
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<Long, Point>(closestCentroidId, p);
        }
    }

    public static final class CountAppender implements MapFunction<Tuple2<Long, Point>, Tuple3<Long, Point, Long>> {

        @Override
        public Tuple3<Long, Point, Long> map(Tuple2<Long, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Long, Point, Long>> {

        @Override
        public Tuple3<Long, Point, Long> reduce(Tuple3<Long, Point, Long> val1, Tuple3<Long, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    public static final class CentroidAverager implements MapFunction<Tuple3<Long, Point, Long>, Point> {

        @Override
        public Point map(Tuple3<Long, Point, Long> value) {
            return new Point(value.f0, value.f1.div(value.f2));
        }
    }
}
