package ro.tucn.util;

import scala.Tuple3;

/**
 * Created by Liviu on 4/6/2017.
 */
public class Utils {

    public static String intToString(int n) {
        return String.format("%05d", n);
    }

    public static double euclideanDistance(Tuple3<Integer, Double, Double> point1,
                                           Tuple3<Integer, Double, Double> point2) {
        return Math.sqrt((point1._2() - point2._2()) * (point1._2() - point2._2())
                + (point1._3() - point1._3()) * (point2._3() - point2._3()));
    }
}
