package ro.tucn.spark.operator;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import ro.tucn.kMeans.Point;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Liviu on 6/24/2017.
 */
public class JavaDStreamWrapper<T> implements Serializable {

    public JavaDStream<T> wrappedStream;

    public JavaDStreamWrapper(JavaDStream<T> stream) {
        this.wrappedStream = stream;
    }

    public static List<Point> collect;

    public JavaDStreamWrapper() {

    }

    public static void getList(JavaDStream<Point> centroids) {
        Fnct<Point> f = new Fnct<Point>();
        centroids.map(f);
        collect = f.collect;
    }

    private static class Fnct<Point> implements Function<Point, Point> {

        public List<Point> collect;

        @Override
        public Point call(Point o) throws Exception {
            collect.add(o);
            return null;
        }
    }
}
