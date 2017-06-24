package ro.tucn.spark.operator;

import org.apache.spark.api.java.function.Function;

/**
 * Created by Liviu on 6/24/2017.
 */
public class VoidFunctionn<Point> implements Function<Point, Point> {

    public SerializableArrayList<Point> centroids;

    @Override
    public Point call(Point t) throws Exception {
        
        return centroids.get(0);
    }

}
