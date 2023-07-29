package org.jjyc;

import org.apache.spark.sql.SparkSession;
import java.util.Map;

public class Utils {

    public static SparkSession getSparkSession(String appName, String master, Map<String, String> props){
        SparkSession.Builder builder = SparkSession.builder()
                .master(master)
                .appName(appName);

        props.forEach(builder::config);
        return builder
                .enableHiveSupport()
                .getOrCreate();
    }
}
