package ro.tucn.spark.operator;

import org.apache.log4j.Logger;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import ro.tucn.exceptions.UnsupportOperatorException;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.BaseOperator;
import ro.tucn.operator.StreamOperator;
import ro.tucn.operator.StreamPairOperator;
import ro.tucn.spark.operator.stream.SparkStreamPairOperator;
import scala.Tuple2;

import java.util.Arrays;

import static ro.tucn.exceptions.ExceptionMessage.FAILED_TO_CAST_OPERATOR_MSG;

/**
 * Created by Liviu on 4/8/2017.
 */
public class SparkStreamOperator<T> extends StreamOperator<T> {

    private static final Logger logger = Logger.getLogger(SparkStreamOperator.class);

    JavaDStream<T> dStream;
    private boolean firstKMeanClustering = true;
    private StreamingKMeans model;

    public SparkStreamOperator(JavaDStream<T> stream, int parallelism) {
        super(parallelism);
        dStream = stream;

    }

    @Override
    public StreamPairOperator<String, Integer> wordCount() {
        JavaDStream<String> stringJavaDStream = dStream.flatMap(x -> Arrays.asList(((String) x).split(" ")).iterator());
        JavaPairDStream<String, Integer> tIntegerJavaPairDStream = stringJavaDStream.mapToPair(s -> new Tuple2(s, 1));
        JavaPairDStream<String, Integer> tIntegerJavaPairDStream1 = tIntegerJavaPairDStream.reduceByKey((i1, i2) -> i1 + i2);
        return new SparkStreamPairOperator(tIntegerJavaPairDStream1, parallelism);
    }

    @Override
    public void kMeansCluster(StreamOperator<T> centroidsOperator) throws WorkloadException {
        checkOperatorType(centroidsOperator);

        JavaDStream<Point> points = (JavaDStream<Point>) this.dStream;
        JavaDStream<Point> centroids = (JavaDStream<Point>) ((SparkStreamOperator<Point>) centroidsOperator).dStream;

        JavaDStream<Vector> pointsVector = points.map(p -> Vectors.dense(p.getCoordinates()));
        pointsVector.print();
        JavaDStream<Vector> centroidsVector = centroids.map(c -> Vectors.dense(c.getCoordinates()));
        centroidsVector.print();

        double[] weights = getEqualWeights(2);
        Vector[] initCentroids = initCentroids(2);

        if (firstKMeanClustering) {
            model = new StreamingKMeans()
                    .setK(2)
                    .setDecayFactor(1)
                    .setInitialCenters(initCentroids, weights);
            firstKMeanClustering = false;
            model.trainOn(pointsVector.dstream());
        }

        JavaPairDStream<Point, Vector> pointVectorJavaPairDStream = points.mapToPair(point -> {
            Vector coordinatesVector = Vectors.dense(point.getCoordinates());
            return new Tuple2<Point, Vector>(point, coordinatesVector);
        });

        JavaPairDStream<Point, Integer> pointIntegerJavaPairDStream = model.predictOnValues(pointVectorJavaPairDStream);
        pointIntegerJavaPairDStream.print();

        Vector[] vectors = model.latestModel().clusterCenters();
        for (int i = 0; i < vectors.length; i++) {
            logger.info("out: " + vectors[i]);
        }
    }

    private Vector[] initCentroids(int n) {
        Vector[] initCentroids = new Vector[2];
        double[] coordinates = new double[2];
        coordinates[0] = 40.5;
        coordinates[1] = -40.5;
        initCentroids[0] = Vectors.dense(coordinates);
        coordinates = new double[2];
        coordinates[0] = -10.5;
        coordinates[1] = 10.5;
        initCentroids[1] = Vectors.dense(coordinates);
        return initCentroids;
    }

    private double[] getEqualWeights(int n) {
        double[] weights = new double[n];
        double weightValue = 1.0 / n;
        for (int i = 0; i < n; i++) {
            weights[i] = weightValue;
        }
        return weights;
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

    private void checkOperatorType(StreamOperator<T> centroids) throws WorkloadException {
        if (!(centroids instanceof SparkStreamOperator)) {
            throw new WorkloadException(FAILED_TO_CAST_OPERATOR_MSG + getClass().getSimpleName());
        }
    }
}