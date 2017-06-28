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
    private AbstractSender KMeansSenderKafka;

    private List<Point> centroids;
    private static long totalPoints;

    public KMeansGenerator(String dataMode, int entitiesNumber) {
        super(entitiesNumber);
        initialize(dataMode);
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

    private void initializeKafkaMessageSenderWithSmallBuffer() {
        KMeansSenderKafka = new KMeansSenderKafka();
        ((AbstractKafkaSender)KMeansSenderKafka).initializeSmallBufferProducer(bootstrapServers);
    }

    private void shutdownSender() {
        ((AbstractKafkaSender)KMeansSenderKafka).close();
    }

    private void submitPoint(Point point) {
        KMeansSenderKafka.send(point);
    }

    @Override
    protected void submitData(int sleepFrequency) {
        KMeansSenderKafka.setTopic(POINT);
        for (int i = 0; i < totalPoints; i++) {
            Point point = KMeansCreator.getNewPoint(centroids);
            submitPoint(point);
            performanceLog.logThroughputAndLatency(TimeHelper.getNanoTime());
            TimeHelper.temporizeDataGeneration(sleepFrequency, i);
        }
        KMeansSenderKafka.setTopic(CENTROID);
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
        } else if (dataMode.equalsIgnoreCase(DataMode.STREAMING)) {
            initializeOfflineMessageSender();
        }
    }

    private void initializeOfflineMessageSender() {
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