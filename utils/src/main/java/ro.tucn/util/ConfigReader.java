package ro.tucn.util;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/6/2017.
 */
public class ConfigReader {

    public static Properties getPropertiesFromResourcesFile(String fileName) throws IOException {
        Properties properties = null;
        properties = new Properties();
        properties.load(ConfigReader.class.getClassLoader().getResourceAsStream(fileName));
        return properties;
    }
}