package ro.tucn.generator.helper;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import ro.tucn.kMeans.Point;

import java.util.List;
import java.util.Random;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansHelper {

    private static final int ID_LOWER_BOUND = 1000;

    private Random centroidRandom;
    private RandomGenerator randomGenerator;
    private RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

    private int dimension;
    private double[] means; // origin point
    private double[][] covariances;

    public Point getNewPoint(List<Point> centroids) {
        MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(randomGenerator, means, covariances);
        double[] location = distribution.sample();
        int centroidIndex = centroidRandom.nextInt(centroids.size());
        for (int i = 0; i < dimension - 1; i++) {
            location[i] += centroids.get(centroidIndex).location[i];
        }
        location[dimension - 1] += centroids.get(centroidIndex).location[dimension - 1];
        int pointId = randomDataGenerator.nextInt(ID_LOWER_BOUND, 10000);
        return new Point(pointId, location);
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
}
