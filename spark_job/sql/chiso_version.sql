CREATE TABLE IF NOT EXISTS chiso_version (
	ngay_baocao date NOT NULL,
	ma_chiso varchar(50) NOT NULL,
	version bigint,
	updated_at bigint 
	CONSTRAINT pk_chiso_version PRIMARY KEY (ngay_baocao, ma_chiso)
);
