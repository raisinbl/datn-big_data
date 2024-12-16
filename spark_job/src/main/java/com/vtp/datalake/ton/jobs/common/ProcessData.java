package com.vtp.datalake.ton.jobs.common;

import com.vtp.datalake.ton.connection.SparkConnection;
import lombok.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.collection.mutable.WrappedArray;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class ProcessData {
    private static final Logger LOGGER = LogManager.getLogger(ProcessData.class);
    static LocalDateTime now = LocalDateTime.now();
    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static DateTimeFormatter formatterday = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static String timeTinhToan = now.format(formatter);
    static String ngaybaocao = now.format(formatterday);

    private static StructType hanhTrinhType = new StructType(new StructField[]{
            DataTypes.createStructField("TRANG_THAI", DataTypes.IntegerType, true),
            DataTypes.createStructField("MA_BUUCUC_HT", DataTypes.StringType, true),
            DataTypes.createStructField("PHAT_THANHCONG", DataTypes.IntegerType, true),
            DataTypes.createStructField("TIME_TAC_DONG", DataTypes.LongType, true),
    });

    private static UDF3<WrappedArray<String>, WrappedArray<Integer>, WrappedArray<Long>, Row> processHanhTrinh() {
        return new UDF3<WrappedArray<String>, WrappedArray<Integer>, WrappedArray<Long>, Row>() {
            @Override
            public Row call(WrappedArray<String> maBuuCucSeq, WrappedArray<Integer> trangThaiSeq, WrappedArray<Long> thoiGianSeq) throws Exception {
                List<String> maBuuCucs = scala.collection.JavaConverters.seqAsJavaList(maBuuCucSeq);
                List<Integer> trangThais = scala.collection.JavaConverters.seqAsJavaList(trangThaiSeq);
                List<Long> thoiGians = scala.collection.JavaConverters.seqAsJavaList(thoiGianSeq);
                int phatThanhCong = 0;
                Long tgTacDong = null;
                String maBuuCuc = null;
                Integer trangThai = 0;
                List<TrangThai> sortTrangThais = new ArrayList<>();
                for (int i = 0; i < thoiGians.size(); i++) {
                    TrangThai model = TrangThai.builder()
                            .trangThai(trangThais.get(i))
                            .thoigian(thoiGians.get(i))
                            .mabuucuc(maBuuCucs.get(i))
                            .build();
                    sortTrangThais.add(model);
                }
                Collections.sort(sortTrangThais, new Comparator<TrangThai>() {
                    public int compare(TrangThai o1, TrangThai o2) {
                        if (o1.thoigian == o2.thoigian) {
                            return Integer.compare(o1.trangThai, o2.trangThai);
                        }
                        return Long.compare(o2.thoigian, o1.thoigian);
                    }
                });
                TrangThai maxtrangThai = sortTrangThais.get(0);// day la trang thai cuoi cung cua don
                tgTacDong = maxtrangThai.thoigian;
                trangThai = maxtrangThai.trangThai;
                maBuuCuc = maxtrangThai.mabuucuc;
                for (TrangThai a : sortTrangThais) {
                    // lay don phat thanh cong
                    if (a.trangThai == 501 || a.trangThai == 503 || a.trangThai == 504 || a.trangThai == 201 || a.trangThai == 107 ) {
                        phatThanhCong = 1;
                    }
                    if (trangThai > 500){
                        if (a.trangThai == 500){
                            maBuuCuc = a.mabuucuc;

                        }
                    }
                }
                return RowFactory.create(trangThai,maBuuCuc,phatThanhCong,tgTacDong);
            }
        };
    }

    public static Dataset<Row> processHanhTrinh(Dataset<Row> HANH_TRINH_ALL) {
        SparkConnection.getSparkSession().udf().register("processHanhTrinh", processHanhTrinh(), hanhTrinhType);
        /** LỌC TRẠNG THÁI THEO ID BẢNG VÀ THỜI GIAN **/
        Dataset<Row> preHANHTRINH_ALL = HANH_TRINH_ALL
                .selectExpr("MA_VANDON MA_PHIEUGUI","MA_BUUCUC","cast(TRANG_THAI as int) TRANG_THAI","cast(THOI_GIAN as bigint) THOI_GIAN","NHANVIENPHAT", "sessionId")
                .filter(col("TRANG_THAI").isNotNull());
        WindowSpec window1 = Window.partitionBy("MA_PHIEUGUI","TRANG_THAI", "THOI_GIAN").orderBy(col("sessionId").desc());
        Dataset<Row> removeDuplicates = preHANHTRINH_ALL.withColumn("row_number",row_number().over(window1)).filter(col("row_number").equalTo(1))
                .na().fill("null");
        removeDuplicates.persist();
        /** Lọc trạng thái >=500 **/
        Dataset<Row> ttLonHon500 = removeDuplicates.select("MA_PHIEUGUI","TRANG_THAI").where("TRANG_THAI >= 500").dropDuplicates();
        Dataset<Row> ttMaxNhoHon50 =removeDuplicates.as("a").join(ttLonHon500.as("b"),expr("a.MA_PHIEUGUI = b.MA_PHIEUGUI"),"leftAnti");
        Dataset<Row> preData = ttMaxNhoHon50.groupBy("MA_PHIEUGUI")
                .agg(
                        collect_list("MA_BUUCUC").as("MA_BUUCUCS"),
                        collect_list("TRANG_THAI").as("TRANG_THAIS"),
                        collect_list("THOI_GIAN").as("THOI_GIANS")
                );
        Dataset<Row> processData = preData.as("ht")
                .withColumn("htNew", functions.callUDF("processHanhTrinh",col("MA_BUUCUCS"),col("TRANG_THAIS"),col("THOI_GIANS")))
                .selectExpr("ht.MA_PHIEUGUI","htNew.*")
                ;
        WindowSpec window2 = Window.partitionBy(("MA_PHIEUGUI")).orderBy(col("TIME_TAC_DONG").desc(),col("TRANG_THAI").desc());
        Dataset<Row> preDf = processData.withColumn("row_number",row_number().over(window2)).filter("row_number = 1")
                .na().fill("NULL");
        /** Loại đơn phát thành công **/
        Dataset<Row> df = preDf
                .where("PHAT_THANHCONG != 1  ")
                .where("TRANG_THAI = 400")
                .withColumn("TIME_TINH_TOAN", lit(timeTinhToan))
                .withColumn("NGAY_BAOCAO",lit(ngaybaocao))
                .drop("PHAT_THANHCONG");
        removeDuplicates.unpersist();
        return df;
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
class TrangThai implements Serializable {
    Long thoigian;
    int trangThai;
    String mabuucuc;
    String nhanvienphat;
}
