package com.vtp.datalake.ton.connection;

import com.vtp.datalake.ton.config.Configuration;
import org.apache.spark.sql.SparkSession;

import static com.vtp.datalake.ton.config.ConfigurationFactory.getConfigInstance;


public class SparkConnection {

    private static Configuration config;
    private static SparkSession spark;


    public static SparkSession getSparkSession(){
        if (spark == null){
            config = getConfigInstance();
            spark = SparkSession
                    .builder().master(config.getConfig("SPARK_MASTER"))
                    .appName("ton_chua_pcp_v2")
                    .getOrCreate();
            spark.sparkContext().setLogLevel("WARN");
        }
        return spark;
    }
}
