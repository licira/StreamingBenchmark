package ro.tucn.workload;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.userfunctions.UserFunctions;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.WorkloadOperator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liviu on 4/15/2017.
 */
public class KMeans extends Workload {

    private static final Logger logger = Logger.getLogger(KMeans.class);

    private List<Point> initCentroids;
    private int centroidsNumber;
    private int dimension;

    public KMeans(OperatorCreator creator) throws WorkloadException {
        super(creator);
        centroidsNumber = Integer.parseInt(properties.getProperty("centroids.number"));
        dimension = Integer.parseInt(properties.getProperty("point.dimension", "2"));
        initCentroids = loadInitCentroids();
    }

    public void process() throws WorkloadException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        try {
            WorkloadOperator<Point> points = getPointStream("source", "topic1");
            points.iterative(); // points iteration
            WorkloadOperator<Point> assignedPoints = points.map(UserFunctions.assign, initCentroids, "assign", Point.class);
            WorkloadOperator<Point> centroids = assignedPoints
                    .mapToPair(UserFunctions.pointMapToPair, "mapToPair")
                    .reduceByKey(UserFunctions.pointAggregator, "aggregator")
                    .map(UserFunctions.computeCentroid, "centroid", Point.class);
            points.closeWith(centroids, true);
            centroids.sink();
            //assignedPoints.print();
            points.print();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private List<Point> loadInitCentroids() {
        List<Point> centroids = new ArrayList();
        BufferedReader br = null;
        InputStream stream = null;
        try {
            String sCurrentLine;
            stream = this.getClass().getClassLoader().getResourceAsStream("init-centroids.txt");

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
        return centroids;
    }
}
