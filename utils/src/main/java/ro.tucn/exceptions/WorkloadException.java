package ro.tucn.exceptions;

import java.io.Serializable;

/**
 * Created by yangjun.wang on 16/10/15.
 */
public class WorkloadException extends Exception implements Serializable {

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
