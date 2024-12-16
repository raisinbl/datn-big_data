package com.vtp.datalake.ton.jobs.model.schema;

import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class Schema {
    public static final StructType phieuGuiSchema = new StructType()
            .add("MA_PHIEUGUI", StringType, true)
            .add("MA_BUUCUC_GOC", StringType, true)
            .add("MA_BUUCUC_PHAT", StringType, true)
            .add("NGAY_GUI_BP", StringType, true)
            .add("TONG_CUOC_VND", StringType, true)
            .add("NGAY_NHAP_MAY", StringType, true)
            .add("TRONG_LUONG", StringType, true)
            .add("THU_HO", StringType, true)
            .add("MA_LOAI_HANGHOA", StringType, true)
            .add("sessionId", StringType, true)
            ;

    public static final StructType hanhTrinhAllSchema = new StructType()
            .add("MA_VANDON", StringType, true)
            .add("TRANG_THAI", StringType, true)
            .add("THOI_GIAN", StringType, true)
            .add("MA_BUUCUC", StringType, true)
            .add("NHANVIENPHAT", StringType, true)
            .add("sessionId", StringType, true)
            ;

    public static final StructType mappingCnSchema = new StructType()
            .add("MA_BUUCUC", StringType, true)
            .add("MA_CN", StringType, true)
            .add("MA_QUANHUYEN", StringType, true)
            .add("TEN_QUANHUYEN", StringType, true)
            ;

}
