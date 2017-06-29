package ro.tucn.kMeans;

import java.io.Serializable;

/**
 * Created by Liviu on 4/6/2017.
 */
public class Point implements Serializable {

    public Long id; // Centroid id: 0, 1, 2, ...
    public double[] coordinates;
    private Long timestamp;

    public Point() { }

    public Point(Long id, double[] coordinates) {
        this.id = id;
        this.coordinates = coordinates;
        this.timestamp = System.nanoTime();
    }

    public Point(Long id, double[] l, long timestamp) {
        this(id, l);
        this.timestamp = timestamp;
    }

    public Point(double[] l) {
        this(-1L, l);
    }

    public Point(double[] l, long timestamp) {
        this(-1L, l);
        this.timestamp = timestamp;
    }

    public Point(Long id, Point point) {
        this.id = id;
        this.coordinates = point.getCoordinates();
    }

    public int dimension() {
        return this.coordinates.length;
    }

    public boolean isCentroid() {
        return this.id >= 0;
    }

    public long getTime() {
        return this.timestamp;
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt(distanceSquaredTo(other));
    }

    public double distanceSquaredTo(Point other) {
        double squareSum = 0;
        double[] otherCoordinates = other.getCoordinates();
        for (int i = 0; i < this.coordinates.length; ++i) {
            squareSum += Math.pow(this.coordinates[i] - otherCoordinates[i], 2);
        }
        return squareSum;
    }

    public Point add(Point point) {
        double[] additionPointCoordinates = point.getCoordinates();
        Point result = new Point();
        double coordinates[] = new double[this.coordinates.length];
        for (int i = 0; i < this.coordinates.length; i++) {
            coordinates[i] = this.coordinates[i] + additionPointCoordinates[i];
        }
        result.setCoordinates(coordinates);
        return result;
    }

    public Point div(Long divider) {
        Point result = new Point();
        double coordinates[] = new double[this.coordinates.length];
        for (int i = 0; i < this.coordinates.length; ++i) {
            coordinates[i] = this.coordinates[i] / divider;
        }
        result.setCoordinates(coordinates);
        return result;
    }

    public String toString() {
        String str = "";
        str += "id: " + id + " ";
        str += "coordinates: [";
        if (coordinates != null) {
            int dimensions = this.coordinates.length;
            int i;
            for (i = 0; i < dimensions - 1; i++) {
                str += this.coordinates[i] + ", ";
            }
            str += this.coordinates[i];
        }
        str += "] ";
        str += "timestamp: " + timestamp;
        return str;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }
}
