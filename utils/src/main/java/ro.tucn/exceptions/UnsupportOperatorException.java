package ro.tucn.exceptions;

/**
 * Created by Liviu on 4/5/2017.
 */
public class UnsupportOperatorException extends Exception {

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
