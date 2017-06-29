package ro.tucn.util;

import ro.tucn.exceptions.DurationException;

import java.util.concurrent.TimeUnit;

import static ro.tucn.exceptions.ExceptionMessage.UNSUPPORTED_TIME_UNIT_MSG;

/**
 * Created by Liviu on 4/8/2017.
 */
public class TimeDuration {

    private TimeUnit unit;
    private long length;

    public TimeDuration(TimeUnit timeUnit, long timeLength) throws DurationException {
        switch (timeUnit) {
            case MILLISECONDS:
                break;
            case SECONDS:
                break;
            case MINUTES:
                break;
            default:
                throw new DurationException(UNSUPPORTED_TIME_UNIT_MSG);
        }
        this.unit = timeUnit;
        this.length = timeLength;
    }

    public static long getSeconds(TimeDuration durations) {
        long seconds = 0L;
        switch (durations.getUnit()) {
            case MILLISECONDS:
                seconds = durations.getLength() / 1000L;
                break;
            case SECONDS:
                seconds = durations.getLength();
                break;
            case MINUTES:
                seconds = durations.getLength() * 1000L;
                break;
        }
        return seconds;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public long getLength() {
        return length;
    }

    public long nanosToSeconds() {
        long seconds = this.length;
        switch (unit) {
            case MILLISECONDS:
                seconds = this.length / 1000L;
                break;
            case SECONDS:
                seconds = this.length;
                break;
            case MINUTES:
                seconds = this.length * 60L;
                break;
        }
        return seconds;
    }

    public long toMilliSeconds() {
        long milliseconds = this.length;
        switch (unit) {
            case MILLISECONDS:
                milliseconds = this.length;
                break;
            case SECONDS:
                milliseconds = this.length * 1000L;
                break;
            case MINUTES:
                milliseconds = this.length * 1000L * 60L;
                break;
        }
        return milliseconds;
    }

    public boolean equals(TimeDuration timeDurations) {
        return (this.getLength() == timeDurations.getLength() && this.getUnit().equals(timeDurations.getUnit()));
    }

    public static double nanosToSeconds(long nanoSeconds) {
        return (double) nanoSeconds / 1000000000.0;
    }

    public static double millisToSeconds(long milliSeconds) {
        return (double) milliSeconds / 1000000.0;
    }
}
