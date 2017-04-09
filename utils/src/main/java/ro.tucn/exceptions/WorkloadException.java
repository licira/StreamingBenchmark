package ro.tucn.exceptions;

/**
 * Created by Liviu on 4/8/2017.
 */
public class WorkloadException extends Exception {

    public WorkloadException(String message) {
        super(message);
    }

    public WorkloadException() {
        super();
    }

    public WorkloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkloadException(Throwable cause) {
        super(cause);
    }

}
