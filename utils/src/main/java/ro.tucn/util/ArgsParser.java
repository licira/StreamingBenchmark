package ro.tucn.util;

import java.util.HashMap;

/**
 * Created by Liviu on 6/25/2017.
 */
public class ArgsParser {

    private static String wrongArgsSyntaxMsg = "Wrong Argument Syntax Error: "
            + "Arguments must have the following syntax: "
            + "[-parameter] [value].";
    private static String wrongArgsNumberMsg = "Wrong Arguments Number Error: "
            + "The number of parameters must be equal to the number of values. ";

    public static HashMap<String, String> parseArgs(String[] args) {
        int length = args.length;
        if (length == 0) {
            return null;
        }
        if (((length % 2) != 0)) {
            throw new RuntimeException(wrongArgsNumberMsg);
        }
        for (int i = 0; i < length; i++) {
            if ((i % 2) == 0) {
                if (args[i].charAt(0) != '-') {
                    throw new RuntimeException(wrongArgsSyntaxMsg);
                }
            } else {
                if (args[i].charAt(0) == '-') {
                    throw new RuntimeException(wrongArgsSyntaxMsg);
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

    private static int tryGetIntValueByKey(String key, HashMap<String, String> paramsWithValues) {
        String value = getValueByKey(key, paramsWithValues);
        int intValue = 0;
        try {
            intValue = Integer.parseInt(value);
        } catch (Exception e) { }
        return intValue;
    }

    private static String getValueByKey(String key, HashMap<String, String> paramsWithValues) {
        return paramsWithValues.get(key);
    }
}
