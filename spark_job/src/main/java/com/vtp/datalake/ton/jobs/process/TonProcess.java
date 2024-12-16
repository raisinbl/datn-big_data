package com.vtp.datalake.ton.jobs.process;

import com.vtp.datalake.ton.jobs.common.ProcessData;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;


public class TonProcess {
    private static final Logger LOGGER = LogManager.getLogger(ProcessData.class);
    static LocalDateTime now = LocalDateTime.now();
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static DateTimeFormatter formatterday = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static String ngaybaocao = now.format(formatterday);

    public static Dataset<Row> ProcessTonChuaPCP(Dataset<Row> preHanhTrinh, Dataset<Row> prePhieuGui, Dataset<Row> mappingCN){
        /** Lấy thông tin từ phiếu gửi  **/
        Dataset<Row> joinPhieuGui = preHanhTrinh.as("a").join(prePhieuGui.as("b"),expr("a.MA_PHIEUGUI = b.MA_PHIEUGUI"),"inner")
                .selectExpr("a.*","b.MA_BUUCUC_GOC","b.MA_BUUCUC_PHAT","b.NGAY_GUI_BP","b.TONG_CUOC_VND","b.NGAY_NHAP_MAY","b.TRONG_LUONG","b.MA_LOAI_HANGHOA","b.THU_HO")
                .withColumnRenamed("THU_HO","TIEN_COD")
                .withColumnRenamed("TONG_CUOC_VND","TIEN_CUOC")
                .withColumnRenamed("MA_LOAI_HANGHOA","LOAI_HH")
                .withColumn("LOAI_HH",expr("case when LOAI_HH ='HH' then 'HANG' when LOAI_HH ='TH' then 'THU' when LOAI_HH ='KH' then 'KIEN' else null end  "))
                .filter("MA_BUUCUC_HT = MA_BUUCUC_PHAT");
        /** mapping chi nhanh **/
        Dataset<Row> getCN = joinPhieuGui.as("a")
                .join(mappingCN.as("b"),expr("a.MA_BUUCUC_HT = b.MA_BUUCUC"),"left")
                .join(mappingCN.as("c"),expr("a.MA_BUUCUC_GOC = c.MA_BUUCUC"),"left")
                .join(mappingCN.as("d"),expr("a.MA_BUUCUC_PHAT = d.MA_BUUCUC"),"left")
                .selectExpr("a.*"
                        ,"b.MA_CN as CHI_NHANH_HT"
                        ,"c.MA_CN as TINH_NHAN","c.MA_QUANHUYEN as HUYEN_NHAN","c.TEN_QUANHUYEN as TEN_HUYEN_NHAN"
                        ,"d.MA_CN as TINH_PHAT","d.MA_QUANHUYEN as HUYEN_PHAT","d.TEN_QUANHUYEN as TEN_HUYEN_PHAT")
                .withColumn("NGAY_BAOCAO", lit(ngaybaocao))
                .drop("row_number");
        return getCN;
    }

    public static Dataset<Row> TonChuaPCPALL(Dataset<Row> ProcessTonChuaPCP) {
        return ProcessTonChuaPCP.na().fill("CHUA_XAC_DINH").groupBy("NGAY_BAOCAO","CHI_NHANH_HT","MA_BUUCUC_HT","LOAI_HH").agg((count("MA_PHIEUGUI").cast("bigint")).as("TONG_SL"))
                .withColumnRenamed("LOAI_HH", "LOAI_HANG");
    }
}
