package ro.tucn.generator.helper;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import ro.tucn.kMeans.Point;

import java.util.List;
import java.util.Random;

/**
 * Created by Liviu on 5/9/2017.
 */
public class KMeansHelper {

    private Random centroidRandom;
    private RandomGenerator pointRandom;
    private int dimension;
    private double[] means; // origin point
    private double[][] covariances;

    public double[] createNewPoints(List<Point> centroids) {
        MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(pointRandom, means, covariances);
        double[] points = distribution.sample();
        int centroidIndex = centroidRandom.nextInt(centroids.size());
        for (int i = 0; i < dimension - 1; i++) {
            points[i] += centroids.get(centroidIndex).location[i];
        }
        points[dimension - 1] += centroids.get(centroidIndex).location[dimension - 1];
        return points;
    }

    public void setCentroidRandom(Random centroidRandom) {
        this.centroidRandom = centroidRandom;
    }

    public void setPointRandom(RandomGenerator pointRandom) {
        this.pointRandom = pointRandom;
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
