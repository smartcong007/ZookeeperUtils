package com.souche.zook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhengcong on 2017/9/29.
 */
public class ConfigUtil {

    private ConfigUtil(){

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtil.class);
    private static Map<String, String> globalConfig = new HashMap<String, String>();

    static {

        try {
            Properties properties = new Properties();
            InputStream in = ClassLoader.getSystemResourceAsStream("global.properties");
            properties.load(in);

            Enumeration<Object> keys = properties.keys();
            while (keys.hasMoreElements()) {

                String key = (String) keys.nextElement();
                String val = properties.getProperty(key);
                globalConfig.put(key, val);

            }
        } catch (IOException e) {
            LOGGER.error("error occurs while loading global properties");
        }


    }

    public static String getVal(String key) {

        return globalConfig.get(key);

    }


}
