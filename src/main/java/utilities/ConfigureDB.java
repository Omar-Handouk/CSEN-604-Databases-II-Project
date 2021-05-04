package utilities;

import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

public class ConfigureDB {

    public static final String configPath = "src/main/resources/DBApp.config";

    public static Hashtable<String, String> loadConfig(String configPath) {
        Hashtable<String, String> config = new Hashtable<>();

        FileReader reader;

        try {
            reader = new FileReader(configPath);

            Properties p = new Properties();
            p.load(reader);

            for (Object s : p.keySet()) {
                config.put((String) s, p.getProperty((String) s));
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        return config;
    }

    public static Hashtable<String, String> loadConfig() {
        return loadConfig(configPath);
    }
}
