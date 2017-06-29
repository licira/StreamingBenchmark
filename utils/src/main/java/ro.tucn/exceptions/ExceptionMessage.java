package ro.tucn.exceptions;

/**
 * Created by Liviu on 6/29/2017.
 */
public class ExceptionMessage {

    public static final String INEXISTING_TOPIC_MESSAGE = "Topic must be specified (using -topic followed by topic value).";
    
    public static final String INEXISTING_MODE_MESSAGE = "Topic must be specified (using -topic followed by topic value).";
    
    public static String WRONG_ARGS_SYNTAX_MSG = "Wrong Argument Syntax Error: "
            + "Arguments must have the following syntax: "
            + "[-parameter] [value].";
    
    public static String WRONG_ARGS_NUMBER_MSG = "Wrong Arguments Number Error: "
            + "The number of parameters must be equal to the number of values.";

    public static String TOPIC_CANNOT_BE_NULL_MSG = "Topic can't be null.";

    public static String INVALID_TIMESTAMP_MSG = "Invalid timestamp: %d. Timestamp should always be non-negative or null.";

    public static String FAILED_TO_CAST_OPERATOR_MSG = "Failed to cast operator to ";

    public static String UNSUPPORTED_TIME_UNIT_MSG = "Unsupported time unit, please use millisecond, second or minute";
}
