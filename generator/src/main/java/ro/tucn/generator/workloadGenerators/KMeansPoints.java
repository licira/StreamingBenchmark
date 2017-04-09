package ro.tucn.generator.workloadGenerators;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import ro.tucn.kMeans.Point;
import ro.tucn.statistics.ThroughputLog;
import ro.tucn.util.Constants;
import ro.tucn.util.Topics;

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
public class KMeansPoints extends Generator {

    private static KafkaProducer<String, String> producer;
    private static long POINT_NUM = 1000;
    private final Logger logger = Logger.getLogger(this.getClass());
    public List<Point> centroids;
    private int dimension;
    private int centroidsNum;
    private double distance;
    private double[] means; // origin point
    private double[][] covariances;

    public KMeansPoints() throws Exception {
        super();
        producer = createSmallBufferProducer();
        initializeWorkloadData();
        TOPIC = Topics.K_MEANS;
    }

    public void generate(int sleepFrequency) throws InterruptedException {
        //logger.info(" generating...");
        generateCentroids();
        centroids = loadCentroids();
        long time = System.currentTimeMillis();
        ThroughputLog throughput = new ThroughputLog(this.getClass().getSimpleName());
        Random centroidRandom = new Random(2342342170123L);
        RandomGenerator pointRandom = new JDKRandomGenerator();
        pointRandom.setSeed(8624214);

        for (long generatedPoints = 0; generatedPoints < POINT_NUM; generatedPoints++) {
            int centroidIndex = centroidRandom.nextInt(centroids.size());
            MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(pointRandom, means, covariances);

            double[] point = distribution.sample();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dimension - 1; i++) {
                point[i] += centroids.get(centroidIndex).location[i];
                sb.append(point[i]).append("\t");
            }
            point[dimension - 1] += centroids.get(centroidIndex).location[dimension - 1];
            sb.append(point[dimension - 1]).append(Constants.TimeSeparator).append(System.currentTimeMillis());

            throughput.execute();
            ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>(TOPIC, sb.toString());
            producer.send(newRecord);
//            System.out.println(sb.toString());
            // control data generate speed
            if (sleepFrequency > 0 && generatedPoints % sleepFrequency == 0) {
                //Thread.sleep(1);
            }
        }
        //logger.info(" done generating");
        producer.close();
        logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time) + "ms");
    }

    /*
    public static void main(String[] args) throws Exception {
        int SLEEP_FREQUENCY = -1;
        if (args.length > 0) {
            SLEEP_FREQUENCY = Integer.parseInt(args[0]);
        }

        // Generate real centroids
//        new KMeansPoints().GenerateCentroids();
        // Generate initialize centroids
//        new KMeansPoints().GenerateInitCentroids();
        // Generate points
        new KMeansPoints().generate(SLEEP_FREQUENCY);
    }
    */
    // Generate 96 real centroids in [-50, 50] for both x and y dimensions
    private void generateCentroids() {
        //logger.info(" generating centroids...");
        Random random = new Random(10000L);
        List<Point> centroids = new ArrayList<Point>();
        for (int i = 0; i < centroidsNum; ) {
            double[] position = new double[dimension];
            for (int j = 0; j < dimension; j++) {
                position[j] = random.nextDouble() * 100 - 50;
            }
            Point p = new Point(position);
            if (!centroids.contains(p)) {
                Point nearestP = null;
                double minDistance = Double.MAX_VALUE;
                for (Point centroid : centroids) {
                    double localDistance = p.distanceSquaredTo(centroid);
                    if (localDistance < minDistance) {
                        minDistance = localDistance;
                        nearestP = centroid;
                    }
                }
                if (nearestP == null || nearestP.distanceSquaredTo(p) > Math.pow(distance, 2)) {
                    centroids.add(p);
                    logger.info(p.positonString());
                    i++;
                }
            }
            //logger.info(" done generating centroids...");
        }

//        KDTree tree = new KDTree();
//        for(int i=0; i<centroidsNum; ){
//            double[] position = new double[dimension];
//            for(int j=0; j<dimension; j++){
//                position[i] = random.nextDouble()*100-50;
//            }
//            Point p = new Point(position);
//            if(!tree.contains(p)) {
//                Point nearestP = tree.nearest(p);
//                if(nearestP == null || nearestP.distanceSquaredTo(p) > Math.pow(distance, 2)) {
//                    tree.insert(p);
//                    System.out.println(p.positonString());
//                    i++;
//                }
//            }
//        }
    }

    protected void initializeWorkloadData() {
        dimension = Integer.parseInt(properties.getProperty("point.dimension", "2"));
        centroidsNum = Integer.parseInt(properties.getProperty("centroids.number", "96"));
        distance = Double.parseDouble(properties.getProperty("centroids.distance", "9"));
        means = new double[dimension];
        covariances = new double[dimension][dimension];
        String covariancesStr = properties.getProperty("covariances", "4,2,2,4");
        String[] covariancesStrs = covariancesStr.split(",");
        if (covariancesStrs.length != dimension * dimension) {
            throw new RuntimeException("Incorrect covariances");
        }
        for (int i = 0; i < dimension; i++) {
            means[i] = 0;
            for (int j = 0; j < dimension; j++) {
                covariances[i][j] = Double.valueOf(covariancesStrs[i * dimension + j]);
            }
        }
    }
    /*
    private void generateInitCentroids() {
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

            logger.info(sb.toString());
        }
    }
    */
    private List<Point> loadCentroids() {
        //logger.info(" loading centroids...");
        List<Point> centroids = new ArrayList<Point>();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = KMeansPoints.class.getClassLoader().getResourceAsStream("centroids.txt");

            br = new BufferedReader(new InputStreamReader(stream));
            while ((sCurrentLine = br.readLine()) != null) {
                String[] strs = sCurrentLine.split(",");
                if (strs.length != dimension) {
                    throw new DimensionMismatchException(strs.length, dimension);
                }
                double[] position = new double[dimension];
                for (int i = 0; i < dimension; i++) {
                    position[i] = Double.valueOf(strs[i]);
                }
                centroids.add(new Point(position));
                //System.out.println(String.valueOf(x) + ", " + String.valueOf(y));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) stream.close();
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        //logger.info(" done loading centroids...");
        return centroids;
    }
}