package ro.tucn.exceptions;

import java.io.Serializable;

/**
 * Window time duration exception
 * cast util.TimeDurations to duration in different platforms
 * Created by jun on 11/3/15.
 */

public class DurationException extends Exception implements Serializable {

    public DurationException(String message) {
        super(message);
    }

    public DurationException() {
        super();
    }

    public DurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DurationException(Throwable cause) {
        super(cause);
    }

}