package ro.tucn.generator.generator;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.creator.entity.KMeansCreator;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.KMeansSender;
import ro.tucn.kMeans.Point;

import java.util.List;
import java.util.Random;

import static ro.tucn.topic.KafkaTopics.CENTROID;
import static ro.tucn.topic.KafkaTopics.POINT;

/**
 * Created by Liviu on 4/5/2017.
 */
public class KMeansGenerator extends AbstractGenerator {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private KMeansCreator KMeansCreator;
    private AbstractMessageSender kMeansSender;

    private List<Point> centroids;
    private static long totalPoints;

    public KMeansGenerator(int entitiesNumber) {
        super(entitiesNumber);
        initialize();
    }

    @Override
    public void generate(int sleepFrequency) {
        centroids = KMeansCreator.generateCentroids();
        //centroids = KMeansCreator.loadCentroids();
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        submitData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        shutdownSender();
    }

    private void initializeHelper() {
        KMeansCreator = new KMeansCreator();
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
            Point point = KMeansCreator.getNewPoint(centroids);
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
        totalPoints = ((entitiesNumber == 0) ? Long.parseLong(properties.getProperty("points.number")) : entitiesNumber);
        long pointIdLowerBound = Long.parseLong(properties.getProperty("point.id.lower.bound"));
        KMeansCreator.setPointIdLowerBound(pointIdLowerBound);
        KMeansCreator.setCentroidRandom(centroidRandomGenerator);
        KMeansCreator.setDimension(dimension);
        KMeansCreator.setDistance(distance);
        KMeansCreator.setCentroidsNo(centroidsNo);
        double[][] covariances = KMeansCreator.getCovariancesFromString(covariancesAsString, means);
        KMeansCreator.initializeMultiderivativeNormalDistribution(pointRandomGenerator, means, covariances);
    }
}