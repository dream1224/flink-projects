package utils;

import java.io.IOException;
import java.util.Properties;

public class PropertyUtils {
    private Properties properties;

    public String getPropertiesValue(String fileName ,String propertiesKey) throws IOException {
        properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(fileName));
        return properties.getProperty(propertiesKey);
    }
}
