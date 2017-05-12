package ro.tucn.util;

import java.io.Serializable;

/**
 * Created by Liviu on 4/8/2017.
 */
public class WithTime<T> implements Serializable, Comparable<WithTime<T>> {

    private T value;
    private long time;

    public WithTime(T v, long time) {
        this.value = v;
        this.time = time;
    }

    public WithTime(T v) {
        this.value = v;
        this.time = System.nanoTime();
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return String.format("%s with time %d", value.toString(), time);
    }

    @Override
    public int compareTo(WithTime<T> o) {
        return 0;
    }
}