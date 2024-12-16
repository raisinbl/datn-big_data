package com.vtp.datalake.ton.config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class PhoenixFactory {
    private static Configuration config;
    public static void save(Dataset<Row> result, String table,Configuration config) {

        result
                .write()
                .format("org.apache.phoenix.spark")
                .mode(SaveMode.Overwrite)
                .option("zkUrl", config.getConfig("PHOENIX.URL"))
                .option("table", table)
                .save();
    }


}
