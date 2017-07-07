package ro.tucn.spark.operator.batch;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.BatchOperator;
import ro.tucn.operator.Operator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;

/**
 * Created by Liviu on 6/25/2017.
 */
public class SparkBatchOperator<T> extends BatchOperator<T> {

    private static final Logger logger = Logger.getLogger(SparkBatchOperator.class);

    private JavaRDD<T> rdd;
    private StreamingKMeans model;

    public SparkBatchOperator(JavaRDD<T> rdd, int parallelism) {
        super(parallelism);
        this.rdd = rdd;
        frameworkName = "SPARK";
    }

    @Override
    public SparkBatchPairOperator<String, Integer> wordCount() {
        performanceLog.disablePrint();
        performanceLog.setStartTime(TimeHelper.getNanoTime());

        JavaRDD<String> sentences = rdd.flatMap(x -> Arrays.asList(((String) x).split(" ")).iterator());
        JavaPairRDD<String, Integer> wordsCountedByOneEach = sentences.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> countedWords = wordsCountedByOneEach.reduceByKey((i1, i2) -> i1 + i2);

        performanceLog.logLatency(TimeHelper.getNanoTime());
        performanceLog.logTotalLatency();
        executionLatency = performanceLog.getTotalLatency();

        return new SparkBatchPairOperator(countedWords, parallelism);
    }

    @Override
    public SparkBatchPairOperator<Long, Point> kMeansCluster(Operator centroidsOperator, int numIterations) throws WorkloadException {
        checkOperatorType(centroidsOperator);

        JavaRDD<Point> points = (JavaRDD<Point>) this.rdd;
        JavaRDD<Point> centroids = (JavaRDD<Point>) ((SparkBatchOperator<Point>) centroidsOperator).rdd;

        int numClusters = centroids.collect().size();

        performanceLog.disablePrint();
        performanceLog.setStartTime(TimeHelper.getNanoTime());

        JavaRDD<Vector> pointsVector = points.map(p -> Vectors.dense(p.getCoordinates()));
        pointsVector.cache();

        KMeansModel finalCentroids = KMeans.train(pointsVector.rdd(), numClusters, numIterations);

        performanceLog.logLatency(TimeHelper.getNanoTime());
        performanceLog.logTotalLatency();
        executionLatency = performanceLog.getTotalLatency();

        for (Vector center: finalCentroids.clusterCenters()) {
            logger.info(center.toString());
        }

        centroidsOperator = new SparkBatchOperator<Point>(centroids, parallelism);

        return null;
    }

    @Override
    public void closeWith(BaseOperator stream, boolean broadcast) throws UnsupportOperatorException {

    }

    @Override
    public void print() {
        List<T> collect = rdd.collect();
        for (T t : collect) {
            logger.info(t.toString());
        }
    }

    private void checkOperatorType(Operator<T> centroids) throws WorkloadException {
        if (!(centroids instanceof SparkBatchOperator)) {
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
        }
    }
}
