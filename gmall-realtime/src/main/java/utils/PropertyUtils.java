package utils;

import common.mapping.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Properties;

@Data
public class PropertyUtils {
    private static Properties properties;
    private static String mysql_ip;

    static  {
        properties = new Properties();
        try {
            properties.load(PropertyUtils.class.getClassLoader().getResourceAsStream(Constants.DB_PROPERTY));
        } catch (IOException e) {
            e.printStackTrace();
        }
        mysql_ip = properties.getProperty("mysql_ip");
    }
}
