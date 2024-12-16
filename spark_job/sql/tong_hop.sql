drop table if exists tong_hop;

create table if not exists tong_hop(
    NGAY_BAOCAO DATE NOT NULL,
    CHI_NHANH_HT varchar,
    MA_BUUCUC_HT varchar,
    LOAI_HANG varchar,
    TONG_SL bigint,
    VERSION bigint,
    UPDATED_AT bigint,
    CONSTRAINT pk_tong_hop PRIMARY KEY (NGAY_BAOCAO, CHI_NHANH_HT, MA_BUUCUC_HT)
);

