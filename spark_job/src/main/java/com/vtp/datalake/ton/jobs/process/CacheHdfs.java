package com.vtp.datalake.ton.jobs.process;

import com.vtp.datalake.ton.jobs.common.ProcessData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class CacheHdfs {

    public static Dataset<Row> CacheThongTinDon(Dataset<Row> HANH_TRINH_ALL,Dataset<Row> PHIEU_GUI,Dataset<Row> MAPPINGCN){
        /** Lấy đơn có tt max 400 **/
        Dataset<Row> preHanhTrinh = ProcessData.processHanhTrinh(HANH_TRINH_ALL);
        /** Process data phieu gui **/
        WindowSpec window1 = Window.partitionBy(col("MA_PHIEUGUI")).orderBy(col("NGAY_NHAP_MAY").desc());
        Dataset<Row> prePhieuGui = PHIEU_GUI.withColumn("row_number", row_number().over(window1)).filter("row_number = 1");
        /** Lấy thông tin đơn và lọc những đơn có mã bưu cục hiện tại = mã bưu cục phát dự kiến **/
        Dataset<Row> preTonChuaPCP = TonProcess.ProcessTonChuaPCP(preHanhTrinh, prePhieuGui,MAPPINGCN)
                .selectExpr("NGAY_BAOCAO","MA_PHIEUGUI","TINH_NHAN","HUYEN_NHAN","TEN_HUYEN_NHAN","TINH_PHAT","HUYEN_PHAT","TEN_HUYEN_PHAT","MA_BUUCUC_GOC"
                        ,"TIME_TAC_DONG","TRANG_THAI","MA_BUUCUC_HT","CHI_NHANH_HT","MA_BUUCUC_PHAT","NGAY_GUI_BP","TIEN_COD"
                        ,"NGAY_NHAP_MAY","TIEN_CUOC","LOAI_HH","TRONG_LUONG");
        /** lọc duplicates **/
        WindowSpec window = Window.partitionBy(col("MA_PHIEUGUI")).orderBy(col("NGAY_BAOCAO").desc());
        Dataset<Row> tonChuaPCP = preTonChuaPCP.withColumn("row_number", row_number().over(window)).filter("row_number = 1");
        return tonChuaPCP;
    }

    public static Dataset<Row> CacheTrangThaiDon (Dataset<Row> HANH_TRINH_ALL){
        /** LỌC TRẠNG THÁI THEO ID BẢNG VÀ THỜI GIAN **/
        Dataset<Row> preHANHTRINH_ALL = HANH_TRINH_ALL
                .filter("MA_BUUCUC is not null")
                .filter("TRANG_THAI is not null");
        WindowSpec window1 = Window.partitionBy("MA_VANDON","TRANG_THAI", "THOI_GIAN").orderBy(col("sessionId").desc());
        Dataset<Row> removeDuplicates = preHANHTRINH_ALL.withColumn("row_number",row_number().over(window1)).filter("row_number = 1")
                .drop("row_number");
        removeDuplicates.persist();
        /** Loại đơn tt >= 500 **/
        Dataset<Row> donTTcuoi = removeDuplicates.where("TRANG_THAI  >= '500' " );
        Dataset<Row> loaidonTTcuoi = removeDuplicates.as("a").join(donTTcuoi.as("b"),expr("a.MA_VANDON = b.MA_VANDON"),"leftAnti")
                .selectExpr("a.*");
        removeDuplicates.unpersist();
        return  loaidonTTcuoi;
    }
}
