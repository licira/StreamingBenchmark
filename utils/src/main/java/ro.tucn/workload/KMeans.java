package ro.tucn.workload;

import org.apache.log4j.Logger;
import ro.tucn.exceptions.WorkloadException;
import ro.tucn.frame.functions.MapFunction;
import ro.tucn.kMeans.Point;
import ro.tucn.operator.GroupedOperator;
import ro.tucn.operator.Operator;
import ro.tucn.operator.OperatorCreator;
import ro.tucn.operator.PairOperator;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Collection;

/**
 * Created by Liviu on 4/15/2017.
 */
public class KMeans extends Workload {

    private static final Logger logger = Logger.getLogger(KMeans.class);

    //private List<Point> initPoints;
    private int centroidsNumber;
    private int dimension;

    public KMeans(OperatorCreator creator) throws WorkloadException {
        super(creator);
        centroidsNumber = Integer.parseInt(properties.getProperty("centroids.number"));
        dimension = Integer.parseInt(properties.getProperty("point.dimension", "2"));
        //initPoints = loadInitPoints();
    }

    public void process() {/*PI*/
        Operator<Point> points = getPointStreamOperator("source", "topic1");
        Operator<Point> centroids = getPointStreamOperator("source", "topic2");
        for(int i =0; i < 10; i++) {
            PairOperator<Integer, Point> pointsWithCentroidId = points.mapToPair(centroids);
            GroupedOperator<Integer, Point> pointsGroupedByCentroidId = pointsWithCentroidId.groupByKey();
            Operator<Point> aggregateCentroids = pointsGroupedByCentroidId.aggregateReduceByKey();
            centroids = aggregateCentroids.map(points);
        }
        /*PairOperator<Integer, Tuple2<Long, Point>> pointMapToPair = pointOperator.mapToPair(UserFunctions.pointMapToPair, "pointMapToPair");
        PairOperator<Integer, Tuple2<Long, Point>> aggregator = pointMapToPair.reduceByKey(UserFunctions.pointAggregator, "aggregator");
        Operator<Point> centroid = aggregator.map(UserFunctions.computePoint, "centroid", Point.class);
        Operator<Point> centroid = pointOperator
                .mapToPair(UserFunctions.pointMapToPair, "pointMapToPair")
                .reduceByKey(UserFunctions.pointAggregator, "aggregator")
                .map(UserFunctions.computePoint, "centroid", Point.class);
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3");
        pointMapToPair.print();
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>4");*/
    }

    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {
        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t._1, t._2, 1L);
        }
    }

    public static final class SelectNearestCenter implements MapFunction<Point, Tuple2<Integer, Point>> {

        private Collection<Point> centroids;

        @Override
        public Tuple2<Integer, Point> map(Point point) {
            double minDistance = Double.MAX_VALUE;
            int closestPointId = -1;
            // check all cluster centers
            for (Point centroid : centroids) {
                // compute distance
                double distance = point.euclideanDistance(centroid);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestPointId = centroid.id;
                }
            }
            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestPointId, point);
        }
    }
    
    /*private List<Point> loadInitPoints() {
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
                if (stream != null) {
                    stream.close();
                }
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return centroids;
    }*/
}
