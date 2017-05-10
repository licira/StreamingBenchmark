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

    public Point createNewPoint(List<Point> centroids) {
        MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(pointRandom, means, covariances);
        double[] location = distribution.sample();
        int centroidIndex = centroidRandom.nextInt(centroids.size());
        for (int i = 0; i < dimension - 1; i++) {
            location[i] += centroids.get(centroidIndex).location[i];
        }
        location[dimension - 1] += centroids.get(centroidIndex).location[dimension - 1];
        int pointId = pointRandom.nextInt(100000) + 1000;
        Point point = new Point(pointId, location);
        return point;
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
