package com.vtp.datalake.ton.jobs;

import com.vtp.datalake.ton.config.Configuration;
import com.vtp.datalake.ton.config.PhoenixFactory;
import com.vtp.datalake.ton.connection.SparkConnection;
import com.vtp.datalake.ton.jobs.process.CacheHdfs;
import com.vtp.datalake.ton.jobs.common.DataLoader;
import com.vtp.datalake.ton.jobs.process.GetTonNgayN;
import com.vtp.datalake.ton.jobs.process.TonProcess;
import com.vtp.datalake.ton.utils.HbaseUtils;
import com.vtp.datalake.ton.utils.HdfsFileSystemApi;
import com.vtp.datalake.ton.utils.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.reflect.ClassTag;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static com.vtp.datalake.ton.config.ConfigurationFactory.getConfigInstance;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class SparkJob {

    private static Utils dtUtils;
    private static Configuration config;
    private static final Logger LOGGER = LogManager.getLogger(SparkJob.class);

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

    static LocalDateTime now = LocalDateTime.now();
    static DateTimeFormatter formatterday = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static String ngaybaocao = now.format(formatterday);


    public static void main(String[] args) throws Exception {
        config = getConfigInstance();
        dtUtils = new Utils();
        DataLoader dl = DataLoader.getDataLoader();
        LOGGER.warn("***** Starting preProcessJob *****");
        String yyyymmdd = dtUtils.getDDMMYYYY(LocalDate.now(), 0, 0, 0);
        System.out.println(yyyymmdd);
        System.out.println(System.currentTimeMillis());
        Long time_check = (long) LocalDateTime.now().getHour();
        System.out.println(config.getConfig("CACHE_TONPHAT") + "/partition=" + yyyymmdd.replace("-", ""));
        String cache_thongtin = config.getConfig("CACHE_TON_CHUA_PCP_THONG_TIN");
        String cache_trangthai = config.getConfig("CACHE_TON_CHUA_PCP_TRANG_THAI");
        String path_cache_trangthai_n = cache_trangthai + "/partition=" + dtUtils.minusDays(LocalDate.now(), 0);
        String path_cache_thongtin_n = cache_thongtin + "/partition=" + dtUtils.minusDays(LocalDate.now(), 0);
        HbaseUtils hbaseUtils = new HbaseUtils();
        Dataset<Row> HANH_TRINH_ALL_N = dl.getHANH_TRINH_ALL_N();
        Dataset<Row> PHIEU_GUI_N = dl.getPHIEU_GUI_N();
        Dataset<Row> HANH_TRINH_ALL = dl.getHANH_TRINH_ALL();
        Dataset<Row> PHIEU_GUI =  dl.getPHIEU_GUI();
        Dataset<Row> MAPPINGCN =  dl.getMAPPINGCN();
        Dataset<Row> CACHE_TRANG_THAI =dl.getCACHE_TRANG_THAI();
        Dataset<Row> CACHE_THONG_TIN = dl.getCACHE_THONG_TIN();
        boolean writeHdfs = true;
        /** check giờ **/
        if( time_check >= config.getConfigLong( "TIME_CHECK_START") && time_check < config.getConfigLong("TIME_CHECK_END")) {
            writeHdfs = true;
        /** check path n**/
        } else {
            if(HdfsFileSystemApi.getInstance().exists(new Path(path_cache_thongtin_n)) && HdfsFileSystemApi.getInstance().exists(new Path(path_cache_trangthai_n))) {
                if (HdfsFileSystemApi.getInstance()
                        .getContentSummary(new Path(path_cache_thongtin_n))
                        .getLength() > 1 &&
                        HdfsFileSystemApi.getInstance()
                                .getContentSummary(new Path(path_cache_trangthai_n))
                                .getLength() > 1 ) {
                    writeHdfs = false;

                }
            } else {
                writeHdfs = true;
            }
        }
        if (writeHdfs || config.getConfigInt("MODE_OVERWRITE").equals(1)) {
            LOGGER.warn("***** WRITE HDFS *****");
            Dataset<Row> cache_thong_tin = CacheHdfs.CacheThongTinDon(HANH_TRINH_ALL,PHIEU_GUI,MAPPINGCN) ;
            Dataset<Row> cache_trang_thai = CacheHdfs.CacheTrangThaiDon(HANH_TRINH_ALL);
            cache_thong_tin.write().mode("overwrite").parquet(config.getConfig("CACHE_TON_CHUA_PCP_THONG_TIN") + "/partition=" + yyyymmdd.replace("-", ""));
            cache_trang_thai.write().mode("overwrite").parquet(config.getConfig("CACHE_TON_CHUA_PCP_TRANG_THAI") + "/partition=" + yyyymmdd.replace("-", ""));
        } else {
            long updated_at = System.currentTimeMillis();
            Long maxOldVersion = hbaseUtils.getMaxOldVersion();
            Long version = maxOldVersion +1;
            Dataset<Row> chiTiet = GetTonNgayN.GetTonPhatNgayN(CACHE_TRANG_THAI,HANH_TRINH_ALL_N,CACHE_THONG_TIN,PHIEU_GUI_N,MAPPINGCN)
                    .withColumn("VERSION",lit(version))
                    .withColumn("UPDATED_AT", lit(updated_at))
                    .drop(col("NGAY_BAOCAO"))
                    .withColumn("TIME_TAC_DONG",col("TIME_TAC_DONG").cast("bigint"))
                    .withColumn("NGAY_GUI_BP",col("NGAY_GUI_BP").cast("bigint"))
                    .withColumn("NGAY_NHAP_MAY",col("NGAY_NHAP_MAY").cast("bigint"))
                    .withColumn("TRANG_THAI",col("TRANG_THAI").cast("bigint"))
                    .withColumn("TIEN_CUOC",col("TIEN_CUOC").cast("bigint"))
                    .withColumn("TIEN_COD",col("TIEN_COD").cast("bigint"))
                    .withColumn("TRONG_LUONG",col("TRONG_LUONG").cast("bigint"));
            chiTiet.persist();
            Dataset<Row> tongHop = TonProcess.TonChuaPCPALL(chiTiet.withColumn("NGAY_BAOCAO",lit(ngaybaocao)))
                    .withColumn("VERSION",lit(version))
                    .withColumn("UPDATED_AT", lit(updated_at));
            Dataset<Row> chiSoVersion = SparkConnection.getSparkSession().createDataset(Arrays.asList(version), Encoders.LONG()).toDF()
                    .withColumnRenamed("value", "VERSION")
                    .withColumn("NGAY_BAOCAO", lit(ngaybaocao))
                    .withColumn("MA_CHISO", lit("ton_chua_pcp"))
                    .withColumn("UPDATED_AT", lit(updated_at));
            LOGGER.warn("***** INSERT CHI TIET *****");
            PhoenixFactory.save(chiTiet,"CHI_TIET",config);
            LOGGER.warn("***** INSERT TONG HOP *****");
            PhoenixFactory.save(tongHop,"TONG_HOP",config);
            LOGGER.warn("***** INSERT CHI SO VERSION *****");
            PhoenixFactory.save(chiSoVersion,"CHISO_VERSION",config);
            LOGGER.warn("***** DELETE VERSION CHI TIET  *****");
            hbaseUtils.deleteVersion_chitiet("CHI_TIET",maxOldVersion);
            LOGGER.warn("***** DELETE VERSION TONG_HOP *****");
            hbaseUtils.deleteVersion_tonghop("TONG_HOP",maxOldVersion,ngaybaocao);
        }
        System.out.println(time_check);
        LOGGER.warn("***** Finished *****");
    }
}