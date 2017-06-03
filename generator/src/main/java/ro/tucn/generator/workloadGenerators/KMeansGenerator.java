package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.helper.KMeansHelper;
import ro.tucn.generator.helper.TimeHelper;
import ro.tucn.generator.sender.AbstractMessageSender;
import ro.tucn.generator.sender.KMeansSender;
import ro.tucn.kMeans.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Liviu on 4/5/2017.
 */
public class KMeansGenerator extends AbstractGenerator {

    private static long POINT_NUM = 10;
    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private KMeansHelper kMeansHelper;
    private AbstractMessageSender kMeansSender;

    private List<Point> centroids;
    private int dimension;
    private int centroidsNo;
    private double distance;

    public KMeansGenerator() {
        super();
        initialize();
    }

    @Override
    public void generate(int sleepFrequency) {
        generateCentroids();
        centroids = loadCentroids();
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

    // Generate 96 real centroids in [-50, 50] for both x and y dimensions
    private void generateCentroids() {
        Random random = new Random(10000L);
        List<Point> centroids = new ArrayList<Point>();
        for (int i = 0; i < centroidsNo; ) {
            double[] position = new double[dimension];
            for (int j = 0; j < dimension; j++) {
                position[j] = random.nextDouble() * 100 - 50;
            }
            Point p = new Point(position);
            if (!centroids.contains(p)) {
                Point nearest = null;
                double minDistance = Double.MAX_VALUE;
                for (Point centroid : centroids) {
                    double localDistance = p.distanceSquaredTo(centroid);
                    if (localDistance < minDistance) {
                        minDistance = localDistance;
                        nearest = centroid;
                    }
                }
                if (nearest == null || (nearest.distanceSquaredTo(p) > distance)) {
                    centroids.add(p);
                    i++;
                }
            }
        }
        /*
        KDTree tree = new KDTree();
        for(int i=0; i<centroidsNo; ){
            double[] position = new double[dimension];
            for(int j=0; j<dimension; j++){
                position[i] = random.nextDouble()*100-50;
            }
            Point p = new Point(position);
            if(!tree.contains(p)) {
                Point nearestP = tree.nearest(p);
                if(nearestP == null || nearestP.distanceSquaredTo(p) > Math.pow(distance, 2)) {
                    tree.insert(p);
                    System.out.println(p.positonString());
                    i++;
                }
            }
        }
        */
    }

    private void shutdownSender() {
        kMeansSender.close();
    }

    private void submitNewPoint() {
        Point point = kMeansHelper.getNewPoint(centroids);
        kMeansSender.send(point);
    }

    private List<Point> loadCentroids() {
        List<Point> centroids = new ArrayList<Point>();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = KMeansGenerator.class.getClassLoader().getResourceAsStream("centroids.txt");
            br = new BufferedReader(new InputStreamReader(stream));
            while ((sCurrentLine = br.readLine()) != null) {
                String[] strs = sCurrentLine.split(",");
                if (strs.length != dimension) {
                    DimensionMismatchException e = new DimensionMismatchException(strs.length, dimension);
                    logger.error(e.getMessage());
                    throw e;
                }
                double[] position = new double[dimension];
                for (int i = 0; i < dimension; i++) {
                    position[i] = Double.valueOf(strs[i]);
                }
                centroids.add(new Point(position));
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (stream != null) stream.close();
                if (br != null) br.close();
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        }
        return centroids;
    }

    private double[][] getCovariancesFromString(String covariancesAsString, double[] means) {
        double[][] covariances = new double[dimension][dimension];
        String[] covariancesStrs = covariancesAsString.split(",");
        if (covariancesStrs.length != (dimension * dimension)) {
            throw new RuntimeException("Incorrect covariances");
        }
        for (int i = 0; i < dimension; i++) {
            means[i] = 0;
            for (int j = 0; j < dimension; j++) {
                covariances[i][j] = Double.valueOf(covariancesStrs[i * dimension + j]);
            }
        }
        return covariances;
    }

    @Override
    protected void submitData(int sleepFrequency) {
        for (long i = 0; i < POINT_NUM; i++) {
            submitNewPoint();
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
        distance = Double.parseDouble(properties.getProperty("centroids.distance"));
        centroidsNo = Integer.parseInt(properties.getProperty("centroids.number"));
        Random centroidRandom = new Random(2342342170123L);
        RandomGenerator pointRandom = new JDKRandomGenerator();
        pointRandom.setSeed(8624214);
        dimension = Integer.parseInt(properties.getProperty("point.dimension"));
        double[] means = new double[dimension];
        String covariancesAsString = properties.getProperty("covariances");
        double[][] covariances = getCovariancesFromString(covariancesAsString, means);

        kMeansHelper.setCentroidRandom(centroidRandom);
        kMeansHelper.setPointRandom(pointRandom);
        kMeansHelper.setDimension(dimension);
        kMeansHelper.setMeans(means);
        kMeansHelper.setCovariances(covariances);
    }

    /*private void generateInitCentroids() {
        centroids = loadCentroids();
        Random random = new Random(12397238947287L);
        List<Point> initCentroids = new ArrayList<Point>();
        RandomGenerator pointRandom = new JDKRandomGenerator();

        for (Point centroid : centroids) {
            MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(pointRandom, means, covariances);

            double[] point = distribution.sample();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimension - 1; i++) {
                point[i] += centroid.location[i];
                sb.append(point[i]).append(", ");
            }
            point[dimension - 1] += centroid.location[dimension - 1];
            sb.append(point[dimension - 1]);

            ro.tucn.logger.info(sb.toString());
        }
    }*/

    /*public static void main(String[] args) throws Exception {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        // Generate real centroids
        //  new KMeansGenerator().GenerateCentroids();
        // Generate initialize centroids
        // new KMeansGenerator().GenerateInitCentroids();
        // Generate points
        new KMeansGenerator().generate(SLEEP_FREQUENCY);
    }*/
}