package com.vtp.datalake.ton.jobs.process;

import com.vtp.datalake.ton.jobs.common.ProcessData;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

public class GetTonNgayN {
    private static final Logger LOGGER = LogManager.getLogger(ProcessData.class);
    static LocalDateTime now = LocalDateTime.now();
    static DateTimeFormatter formatterday = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static String ngaybaocao = now.format(formatterday);

    public static Dataset<Row> GetTonPhatNgayN(Dataset<Row> CACHE_TRANG_THAI,Dataset<Row> HANH_TRINH_ALL_N,Dataset<Row> CACHE_THONG_TIN,Dataset<Row> PHIEU_GUI_N,Dataset<Row> MAPPINGCN){
        /** Gộp trạng thái đơn mới và cũ **/
        Dataset<Row> uniondata = CACHE_TRANG_THAI.unionByName(HANH_TRINH_ALL_N);
        /** Lấy đơn ở TT max 400 **/
        Dataset<Row> preHanhTrinh = ProcessData.processHanhTrinh(uniondata);
        preHanhTrinh.persist();
        /** Lấy thông tin những đơn đang tồn cũ **/
        Dataset<Row> tonCu = CACHE_THONG_TIN.as("a").join(preHanhTrinh.as("b"),col("a.MA_PHIEUGUI").equalTo(col("b.MA_PHIEUGUI")),"inner")
                .selectExpr("a.*","b.TRANG_THAI as TT_MOI","b.MA_BUUCUC_HT as MA_BC_MOI","b.TIME_TAC_DONG as TIME_TAC_DONG_MOI")
                .withColumn("TRANG_THAI",when(col("TT_MOI").isNotNull(),col("TT_MOI")).otherwise("TRANG_THAI"))
                .withColumn("MA_BUUCUC_HT",when(col("MA_BC_MOI").isNotNull(),col("MA_BC_MOI")).otherwise("MA_BUUCUC_HT"))
                .withColumn("TIME_TAC_DONG",when(col("TIME_TAC_DONG_MOI").isNotNull(),col("TIME_TAC_DONG_MOI")).otherwise("TIME_TAC_DONG"))
                .drop(col("NGAY_BAOCAO")).drop(col("TT_MOI")).drop(col("MA_BC_MOI")).drop(col("TIME_TAC_DONG_MOI")).drop(col("ROW_NUMBER"))
                .withColumn("NGAY_BAOCAO", lit(ngaybaocao))
                .where("TRANG_THAI = 400")
                .where("MA_BUUCUC_HT = MA_BUUCUC_PHAT")
                .selectExpr("NGAY_BAOCAO","MA_PHIEUGUI","TINH_NHAN","HUYEN_NHAN","TEN_HUYEN_NHAN","TINH_PHAT","HUYEN_PHAT","TEN_HUYEN_PHAT","MA_BUUCUC_GOC"
                ,"TIME_TAC_DONG","TRANG_THAI","MA_BUUCUC_HT","CHI_NHANH_HT","MA_BUUCUC_PHAT","NGAY_GUI_BP","TIEN_COD"
                ,"NGAY_NHAP_MAY","TIEN_CUOC","LOAI_HH","TRONG_LUONG")
                .withColumn("CHECK",lit(1));
        /** Lấy thông tin những đơn đang tồn mơi **/
        WindowSpec window1 = Window.partitionBy(col("MA_PHIEUGUI")).orderBy(col("NGAY_NHAP_MAY").desc());
        Dataset<Row> prePHIEUGUI = PHIEU_GUI_N.withColumn("row_number",row_number().over(window1)).filter(col("row_number").equalTo(1));
        Dataset<Row> tonMoi = TonProcess.ProcessTonChuaPCP(preHanhTrinh,prePHIEUGUI,MAPPINGCN)
                .selectExpr("NGAY_BAOCAO","MA_PHIEUGUI","TINH_NHAN","HUYEN_NHAN","TEN_HUYEN_NHAN","TINH_PHAT","HUYEN_PHAT","TEN_HUYEN_PHAT","MA_BUUCUC_GOC"
                        ,"TIME_TAC_DONG","TRANG_THAI","MA_BUUCUC_HT","CHI_NHANH_HT","MA_BUUCUC_PHAT","NGAY_GUI_BP","TIEN_COD"
                        ,"NGAY_NHAP_MAY","TIEN_CUOC","LOAI_HH","TRONG_LUONG")
                .withColumn("CHECK",lit(2))
                ;
        /** Gộp đơn tồn cũ và tồn mới = tồn hiện tại **/
        Dataset<Row> unionData = tonCu.unionByName(tonMoi);
        WindowSpec window2 = Window.partitionBy(col("MA_PHIEUGUI")).orderBy(col("CHECK").desc());
        Dataset<Row> data = unionData.withColumn("row_number",row_number().over(window2)).filter(col("row_number").equalTo(1))
                .drop(col("CHECK")).drop(col("ROW_NUMBER"))
            .na().fill("CHUA_XAC_DINH")
        ;
        preHanhTrinh.unpersist();
        return data;
    }
}