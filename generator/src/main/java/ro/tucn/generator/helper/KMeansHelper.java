package ro.tucn.generator.helper;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.workloadGenerators.KMeansGenerator;
import ro.tucn.kMeans.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansHelper {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private static final int ID_LOWER_BOUND = 1000;

    private Random centroidRandom;
    private RandomGenerator randomGenerator;
    private RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

    private int dimension;
    private double[] means; // origin point
    private double[][] covariances;
    private double distance;
    private int centroidsNo;

    public Point getNewPoint(List<Point> centroids) {
        MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(randomGenerator, means, covariances);
        double[] location = distribution.sample();
        int centroidIndex = centroidRandom.nextInt(centroids.size());
        for (int i = 0; i < dimension - 1; i++) {
            location[i] += centroids.get(centroidIndex).coordinates[i];
        }
        location[dimension - 1] += centroids.get(centroidIndex).coordinates[dimension - 1];
        int pointId = generateId();
        return new Point(pointId, location);
    }

    public List<Point> loadCentroids() {
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
                int pointId = generateId();
                centroids.add(new Point(pointId, position));
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

    // Generate 96 real centroids in [-50, 50] for both x and y dimensions
    public List<Point> generateCentroids() {
        Random random = new Random(10000L);
        List<Point> centroids = new ArrayList<Point>();
        for (int i = 0; i < centroidsNo; ) {
            double[] position = new double[dimension];
            for (int j = 0; j < dimension; j++) {
                position[j] = random.nextDouble() * 100 - 50;
            }
            Point point = new Point(position);
            if (!centroids.contains(point)) {
                Point nearest = null;
                double minDistance = Double.MAX_VALUE;
                for (Point centroid : centroids) {
                    double localDistance = point.distanceSquaredTo(centroid);
                    if (localDistance < minDistance) {
                        minDistance = localDistance;
                        nearest = centroid;
                    }
                }
                if (nearest == null || (nearest.distanceSquaredTo(point) > distance)) {
                    point.setId(generateId());
                    centroids.add(point);
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
        return centroids;
    }

    public void getCovariancesFromString(String covariancesAsString, double[] means) {
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
        this.covariances = covariances;
    }

    private int generateId() {
        return randomDataGenerator.nextInt(ID_LOWER_BOUND, 10000);
    }

    public void setCentroidRandom(Random centroidRandom) {
        this.centroidRandom = centroidRandom;
    }

    public void setPointRandom(RandomGenerator pointRandom) {
        this.randomGenerator = pointRandom;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public void setMeans(double[] means) {
        this.means = means;
    }

    public void setCovariances(double[][] covariances) {
        this.covariances = covariances;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setCentroidsNo(int centroidsNo) {
        this.centroidsNo = centroidsNo;
    }
}
