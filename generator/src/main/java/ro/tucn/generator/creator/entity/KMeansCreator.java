package ro.tucn.generator.creator.entity;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.log4j.Logger;
import ro.tucn.generator.workloadGenerators.KMeansGenerator;
import ro.tucn.kMeans.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansCreator {

    private long pointIdLowerBound;
    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    private Random centroidRandom;
    private RandomGenerator randomGenerator = new JDKRandomGenerator();
    private MultivariateNormalDistribution multiderivativeNormalDistribution;

    private int dimension;
    private double distance;
    private int centroidsNo;

    public Point getNewPoint(List<Point> centroids) {
        double[] coordinatesDistribution = multiderivativeNormalDistribution.sample();
        //randomGenerator.setSeed(10000);
        int centroidIndex = centroidRandom.nextInt(centroids.size());
        Point centroid = centroids.get(centroidIndex);
        double[] centroidCoordinates = centroid.getCoordinates();
        double[] pointCoordinates = new double[centroidCoordinates.length];
        for (int i = 0; i < dimension; i++) {
            pointCoordinates[i] = centroidCoordinates[i] + coordinatesDistribution[i];
            DecimalFormat f = new DecimalFormat("##.##");
            pointCoordinates[i] = Double.parseDouble(f.format(pointCoordinates[i]));
        }
        Long pointId = generateId();
        return new Point(pointId, pointCoordinates);
    }

    public List<Point> getNewPoints(List<Point> centroids, long n) {
        List<Point> points = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            points.add(getNewPoint(centroids));
        }
        return points;
    }

    // Generate 96 real centroids in [-50, 50] for both x and y dimensions
    public List<Point> generateCentroids() {
        //randomGenerator.setSeed(10000);
        List<Point> centroids = new ArrayList<Point>();
        for (int i = 0; i < centroidsNo; ) {
            double[] position = new double[dimension];
            for (int j = 0; j < dimension; j++) {
                position[j] = generateCoordinate();
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
        return centroids;
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
                Long pointId = generateId();
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

    public double[][] getCovariancesFromString(String covariancesAsString, double[] means) {
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

    public void initializeMultiderivativeNormalDistribution(RandomGenerator randomGenerator, double[] means, double[][] covariances) {
        this.multiderivativeNormalDistribution = new MultivariateNormalDistribution(randomGenerator, means, covariances);
    }

    private double generateCoordinate() {
        double coordinate;
        coordinate = (randomGenerator.nextInt(2000) - 1000) / 100.0;
        //coordinate = Math.round(coordinate / 100);
        DecimalFormat f = new DecimalFormat("##.##");
        //coordinate = Double.parseDouble(f.format(coordinate));
        coordinate += (coordinate > 0) ? -50 : 50;
        if (coordinate > 0 && coordinate < 10) {
            coordinate += 10;
        } else if (coordinate < 0 && coordinate > -10) {
            coordinate -= 10;
        }
        return coordinate;
    }

    private Long generateId() {
        long id = randomGenerator.nextLong() % pointIdLowerBound;
        if (id < 0) {
            id = -id;
        }
        id += pointIdLowerBound;
        return id;
    }

    public void setCentroidRandom(Random centroidRandom) {
        this.centroidRandom = centroidRandom;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setCentroidsNo(int centroidsNo) {
        this.centroidsNo = centroidsNo;
    }

    public void setPointIdLowerBound(long pointIdLowerBound) {
        this.pointIdLowerBound = pointIdLowerBound;
    }
}
