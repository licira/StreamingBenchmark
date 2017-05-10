package ro.tucn.kMeans;

import java.io.Serializable;

/**
 * Created by Liviu on 4/6/2017.
 */
public class Point implements Serializable {

    public int id; // Centroid id: 0, 1, 2, ...
    public double[] location;
    private long time;

    public Point() { }

    public Point(int id, double[] location) {
        this.id = id;
        this.location = location;
        this.time = System.nanoTime();
    }

    public Point(int id, double[] l, long time) {
        this(id, l);
        this.time = time;
    }

    public Point(double[] l) {
        this(-1, l);
    }

    public Point(double[] l, long time) {
        this(-1, l);
        this.time = time;
    }

    public int dimension() {
        return this.location.length;
    }

    public boolean isCentroid() {
        return this.id >= 0;
    }

    public long getTime() {
        return this.time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    /*public Point add(Point other) throws Exception {
        if (this.location.length != other.location.length) {
            throw new Exception("Dimensions of points are not equal");
        }
        double[] location = new double[this.location.length];
        for (int i = 0; i < this.location.length; ++i) {
            location[i] = this.location[i] + other.location[i];
        }
        return new Point(this.id, location, this.time);
    }*/

    /*public Point mul(long val) {
        double[] location = new double[this.location.length];
        for (int i = 0; i < this.location.length; ++i) {
            location[i] = this.location[i] * val;
        }
        return new Point(this.id, location, this.time);
    }*/

    /*public Point div(long val) {
        double[] location = new double[this.location.length];
        for (int i = 0; i < this.location.length; ++i) {
            location[i] = this.location[i] / val;
        }
        return new Point(this.id, location, this.time);
    }*/

    public double euclideanDistance(Point other) {
        return Math.sqrt(distanceSquaredTo(other));
    }

    public double distanceSquaredTo(Point other) {
        double squareSum = 0;
        for (int i = 0; i < this.location.length; ++i) {
            squareSum += Math.pow(this.location[i] - other.location[i], 2);
        }
        return squareSum;
    }

    public void setLocation(double[] location) {
        this.location = location;
    }

    public String toString() {
        String str = "";
        for (int i = 0; i < this.location.length - 1; ++i) {
            str += this.location[i] + " ";
        }
        str += this.location[this.location.length - 1];
        if (-1 != this.id)
            return id + ":" + str;
        return str;
    }

    public int getId() {
        return id;
    }

    public double[] getLocation() {
        return location;
    }
}
