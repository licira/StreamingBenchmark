package ro.tucn.util;

import java.util.HashMap;

import static ro.tucn.exceptions.ExceptionMessage.*;

/**
 * Created by Liviu on 6/25/2017.
 */
public class ArgsParser {

    public static HashMap<String, String> parseArgs(String[] args) {
        int length = args.length;
        if (length == 0) {
            return null;
        }
        if (((length % 2) != 0)) {
            throw new RuntimeException(WRONG_ARGS_NUMBER_MSG);
        }
        for (int i = 0; i < length; i++) {
            if ((i % 2) == 0) {
                if (args[i].charAt(0) != '-') {
                    throw new RuntimeException(WRONG_ARGS_SYNTAX_MSG);
                }
            } else {
                if (args[i].charAt(0) == '-') {
                    throw new RuntimeException(WRONG_ARGS_SYNTAX_MSG);
                }
            }
        }
        return getParamsWithValuesFromArgs(args);
    }

    private static HashMap<String, String> getParamsWithValuesFromArgs(String[] args) {
        int length = args.length;
        HashMap<String, String> paramsWithValues = new HashMap<String, String>();
        for (int i = 0; i < length; i += 2) {
            paramsWithValues.put(args[i], args[i + 1]);
        }
        return paramsWithValues;
    }

    public static String getTopic(HashMap<String, String> parsedArgs) {
        return parsedArgs.get("-topic");
    }

    public static int getNumberOfGeneratedEntities(HashMap<String, String> paramsWithValues) {
        return tryGetIntValueByKey("-number", paramsWithValues);
    }

    public static int getSleepFrequency(HashMap<String, String> paramsWithValues) {
        return tryGetIntValueByKey("-sleep.frequency", paramsWithValues);
    }

    public static String getMode(HashMap<String, String> parsedArgs) {
        return parsedArgs.get("-mode");
    }

    private static int tryGetIntValueByKey(String key, HashMap<String, String> paramsWithValues) {
        String value = getValueByKey(key, paramsWithValues);
        int intValue = 0;
        try {
            intValue = Integer.parseInt(value);
        } catch (Exception e) {
        }
        return intValue;
    }

    private static String getValueByKey(String key, HashMap<String, String> paramsWithValues) {
        return paramsWithValues.get(key);
    }

    public static void checkParamsValidityForGenerator(HashMap<String, String> paramsAndValues) {
        String topic = ArgsParser.getTopic(paramsAndValues);
        if (topic.isEmpty()) {
            throw new RuntimeException(INEXISTING_TOPIC_MESSAGE);
        }
    }

    public static void checkParamsValidityForTestBed(HashMap<String, String> paramsAndValues) {
        String topic = ArgsParser.getTopic(paramsAndValues);
        String mode = ArgsParser.getMode(paramsAndValues);
        if (topic.isEmpty()) {
            throw new RuntimeException(INEXISTING_TOPIC_MESSAGE);
        }
        if (mode.isEmpty()) {
            throw new RuntimeException(INEXISTING_MODE_MESSAGE);
        }
    }
}
