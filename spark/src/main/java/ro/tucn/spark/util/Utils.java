package ro.tucn.spark.util;

import java.util.Iterator;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import ro.tucn.util.TimeDuration;

/**
 * Created by Liviu on 4/8/2017.
 */
public class Utils {

    public static Duration timeDurationsToSparkDuration(TimeDuration timeDurations) {
        Duration duration = Durations.seconds(1);
        switch (timeDurations.getUnit()) {
            case MILLISECONDS:
                duration = Durations.milliseconds(timeDurations.getLength());
                break;
            case SECONDS:
                duration = Durations.seconds(timeDurations.getLength());
                break;
            case MINUTES:
                duration = Durations.minutes(timeDurations.getLength());
                break;
        }
        return duration;
    }

    public static <E> Iterable<E> iterable(final Iterator<E> iterator) {
        if (iterator == null) {
            throw new NullPointerException();
        }
        return new Iterable<E>() {
            public Iterator<E> iterator() {
                return iterator;
            }
        };
    }
}
