package com.vtp.datalake.ton.jobs.common;

import com.vtp.datalake.ton.config.Configuration;
import com.vtp.datalake.ton.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static com.vtp.datalake.ton.config.ConfigurationFactory.getConfigInstance;
import static com.vtp.datalake.ton.connection.SparkConnection.getSparkSession;
import com.vtp.datalake.ton.jobs.model.schema.Schema;

public class DataLoader {
    private static Configuration config;
    private static SparkSession spark;
    private static Utils dtUtils;
    private Dataset<Row> PHIEU_GUI, PHIEU_GUI_N, MAPPINGCN, HANH_TRINH_ALL, CACHE_THONG_TIN, HANH_TRINH_ALL_N, CACHE_TRANG_THAI;
    private static DataLoader dl;


    public DataLoader() throws IOException {


        spark = getSparkSession();
        dtUtils = new Utils();
        config = getConfigInstance();

        MAPPINGCN = spark.read().option("header", true).schema(Schema.mappingCnSchema).csv(config.getConfig("HDFS_URL.MAPPINGCN")).dropDuplicates();
        PHIEU_GUI =spark.read().schema(Schema.phieuGuiSchema).parquet(dtUtils.getURLDateRange(config.getConfig("HDFS_URL.PHIEU_GUI"),
                config.getConfigInt("NUM_DAYS_PHIEU_GUI_CACHE")));
        PHIEU_GUI_N =spark.read().schema(Schema.phieuGuiSchema).parquet(dtUtils.getURLDateRange(config.getConfig("HDFS_URL.PHIEU_GUI"),
                config.getConfigInt("NUM_DAYS_PHIEU_GUI_N")));
        HANH_TRINH_ALL = spark.read().schema(Schema.hanhTrinhAllSchema).parquet(dtUtils.getURLDateRange(config.getConfig("HDFS_URL.HANHTRINH_ALL"),
                config.getConfigInt("NUM_DAYS_HANHTRINH_ALL_CACHE")));
        HANH_TRINH_ALL_N = spark.read().schema(Schema.hanhTrinhAllSchema).parquet(dtUtils.getURLDateRange(config.getConfig("HDFS_URL.HANHTRINH_ALL"),
                config.getConfigInt("NUM_DAYS_HANHTRINH_ALL_N")));
        String path_cache_trangthai = dtUtils.getUrlLatestDayCache(config.getConfig("CACHE_TON_CHUA_PCP_TRANG_THAI"));
        String path_cache_thongtin = dtUtils.getUrlLatestDayCache(config.getConfig("CACHE_TON_CHUA_PCP_THONG_TIN"));
        CACHE_THONG_TIN = spark.read().parquet(path_cache_thongtin);
        CACHE_TRANG_THAI = spark.read().parquet(path_cache_trangthai);
    }

    public Dataset<Row> getCACHE_THONG_TIN() {
        return CACHE_THONG_TIN;
    }

    public Dataset<Row> getCACHE_TRANG_THAI() {
        return CACHE_TRANG_THAI;
    }

    public Dataset<Row> getPHIEU_GUI_N() {
        return PHIEU_GUI_N;
    }

    public Dataset<Row> getHANH_TRINH_ALL_N() {
        return HANH_TRINH_ALL_N;
    }


    public Dataset<Row> getHANH_TRINH_ALL() {
        return HANH_TRINH_ALL;
    }


    public Dataset<Row> getMAPPINGCN() {
        return MAPPINGCN;
    }


    public Dataset<Row> getPHIEU_GUI() {
        return PHIEU_GUI;
    }

    public static DataLoader getDataLoader() throws IOException {
        if (dl == null) {
            dl = new DataLoader();
        }
        return dl;
    }
}
