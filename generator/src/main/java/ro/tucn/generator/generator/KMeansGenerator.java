package ro.tucn.generator.generator;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.log4j.Logger;
import ro.tucn.DataMode;
import ro.tucn.generator.creator.entity.KMeansCreator;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractSender;
import ro.tucn.generator.sender.kafka.AbstractKafkaSender;
import ro.tucn.generator.sender.kafka.KMeansSenderKafka;
import ro.tucn.generator.sender.offline.KMeansSenderOffline;
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

    private KMeansCreator kMeansCreator;
    private AbstractSender kMeansSender;

    private List<Point> centroids;
    private static long totalPoints;

    public KMeansGenerator(String dataMode, int entitiesNumber) {
        super(entitiesNumber);
        initialize(dataMode);
    }

    @Override
    public void generate(int sleepFrequency) {
        centroids = kMeansCreator.generateCentroids();
        //centroids = KMeansCreator.loadCentroids();
        initializePerformanceLogWithCurrentTime();
        performanceLog.disablePrint();
        submitData(sleepFrequency);
        performanceLog.logTotalThroughputAndTotalLatency();
        shutdownSender();
    }

    private void initializeHelper() {
        kMeansCreator = new KMeansCreator();
    }

    private void initializeKafkaMessageSenderWithSmallBuffer() {
        kMeansSender = new KMeansSenderKafka();
        ((AbstractKafkaSender)kMeansSender).initializeSmallBufferProducer(bootstrapServers);
    }

    private void initializeOfflineMessageSender() {
        kMeansSender = new KMeansSenderOffline();
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
            Point point = kMeansCreator.getNewPoint(centroids);
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
    protected void initialize(String dataMode) {
        initializeHelper();
        initializeMessageSenders(dataMode);
        initializeWorkloadData();
        initializeDataGenerators();
    }

    private void initializeMessageSenders(String dataMode) {
        if (dataMode.equalsIgnoreCase(DataMode.STREAMING)) {
            initializeKafkaMessageSenderWithSmallBuffer();
        } else if (dataMode.equalsIgnoreCase(DataMode.BATCH)) {
            initializeOfflineMessageSender();
        }
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
        kMeansCreator.setPointIdLowerBound(pointIdLowerBound);
        kMeansCreator.setCentroidRandom(centroidRandomGenerator);
        kMeansCreator.setDimension(dimension);
        kMeansCreator.setDistance(distance);
        kMeansCreator.setCentroidsNo(centroidsNo);
        double[][] covariances = kMeansCreator.getCovariancesFromString(covariancesAsString, means);
        kMeansCreator.initializeMultiderivativeNormalDistribution(pointRandomGenerator, means, covariances);
    }
}