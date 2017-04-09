package ro.tucn.exceptions;

/**
 * Created by Liviu on 4/5/2017.
 */
public class DurationException extends Exception {

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