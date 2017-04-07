package ro.tucn.exceptions;

import java.io.Serializable;

/**
 * Created by jun on 27/02/16.
 */
public class UnsupportOperatorException extends Exception implements Serializable {

    public UnsupportOperatorException(String message) {
        super(message);
    }

    public UnsupportOperatorException() {
        super();
    }

    public UnsupportOperatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportOperatorException(Throwable cause) {
        super(cause);
    }

}
