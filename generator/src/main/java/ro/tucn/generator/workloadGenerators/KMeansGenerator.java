package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.helper.KMeansHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.KMeansSender;
import ro.tucn.kMeans.Point;

import java.util.List;
import java.util.Random;

import static ro.tucn.util.Topics.CENTROID;
import static ro.tucn.util.Topics.POINT;

/**
 * Created by Liviu on 4/5/2017.
 */
public class KMeansGenerator extends AbstractGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private KMeansHelper kMeansHelper;
    private AbstractMessageSender kMeansSender;

    private List<Point> centroids;
    private static long totalPoints;

    public KMeansGenerator() {
        super();
        initialize();
    }

    @Override
    public void generate(int sleepFrequency) {
        centroids = kMeansHelper.generateCentroids();
        //centroids = kMeansHelper.loadCentroids();
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        submitData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        shutdownSender();
    }

    private void initializeHelper() {
        kMeansHelper = new KMeansHelper();
    }

    private void initializeMessageSenderWithSmallBuffer() {
        kMeansSender = new KMeansSender();
        kMeansSender.initializeSmallBufferProducer(bootstrapServers);
    }

    private void shutdownSender() {
        kMeansSender.close();
    }

    private void submitPoint(Point point) {
        kMeansSender.send(point);
    }

    @Override
    protected void submitData(int sleepFrequency) {
        kMeansSender.setTopic(POINT);
        for (int i = 0; i < totalPoints; i++) {
            Point point = kMeansHelper.getNewPoint(centroids);
            submitPoint(point);
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
        kMeansSender.setTopic(CENTROID);
        for (int i = 0; i < centroids.size(); i++) {
            submitPoint(centroids.get(i));
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
    }

    @Override
    protected void initialize() {
        initializeHelper();
        initializeMessageSenderWithSmallBuffer();
        initializeWorkloadData();
        initializeDataGenerators();
    }

    @Override
    protected void initializeDataGenerators() {

    }

    @Override
    protected void initializeWorkloadData() {
        double distance = Double.parseDouble(properties.getProperty("centroids.distance"));
        int centroidsNo = Integer.parseInt(properties.getProperty("centroids.number"));
        Random centroidRandomGenerator = new Random();
        RandomGenerator pointRandomGenerator = new JDKRandomGenerator();
        int dimension = Integer.parseInt(properties.getProperty("point.dimension"));
        double[] means = new double[dimension];
        String covariancesAsString = properties.getProperty("covariances");
        totalPoints = Long.parseLong(properties.getProperty("points.number"));
        long pointIdLowerBound = Long.parseLong(properties.getProperty("point.id.lower.bound"));
        kMeansHelper.setPointIdLowerBound(pointIdLowerBound);
        kMeansHelper.setCentroidRandom(centroidRandomGenerator);
        kMeansHelper.setDimension(dimension);
        kMeansHelper.setDistance(distance);
        kMeansHelper.setCentroidsNo(centroidsNo);
        double[][] covariances = kMeansHelper.getCovariancesFromString(covariancesAsString, means);
        kMeansHelper.initializeMultiderivativeNormalDistribution(pointRandomGenerator, means, covariances);
    }
}