package ro.tucn.util;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Liviu on 4/6/2017.
 */
public class ConfigReader {

    public Properties tryGetPropertiesFromResourcesFile(String fileName) {
        Properties properties = null;
        try {
            properties = getPropertiesFromResourcesFile(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        return properties;
    }

    public Properties getPropertiesFromResourcesFile(String fileName) throws IOException {
        Properties properties = null;
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(fileName));
        return properties;
    }
}