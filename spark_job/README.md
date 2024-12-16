# Tồn chưa phân công phát - V2


## 1. Định nghĩa

 - Bưu gửi đang phát sinh trạng thái 400, có bưu cục hiện tại = bưu cục phát dự kiến của đơn. 
 - Không tính đơn đã phát sinh TT101, 107, 201, 501, 503, 504, 516, 517.
 - Không tính đơn có mã phiếu gửi đuôi 1P1, BP, DH. 
 - Không tính đơn có mã phiếu gửi đầu CH, XMG. 
 - Không tính đơn có mã dịch vụ GLK, GCTP (dịch vụ chính). 
 - Không tính đơn có mã dịch vụ BHS, VTQ (dịch vụ chính).
 - Không tính các đơn tồn tại bưu cục TN1, TN2, TN3, TN6, PCNTT, PTKSP, DTHNI, DTHCM.

**Lưu ý**:
- Đối với đơn tách kiện, ghi nhận tồn cho tất cả các mã kiện con. 
- Đối với đơn đang phát sinh trạng thái 202 => Lấy trạng thái gần nhất trước 202 để xét loại tồn. 
- Gán tồn cho chi nhánh, bưu cục hiện tại của bưu gửi.

## 2. Input

Sử dụng thông tin đơn và hành trình lấy từ các bảng tồn core:
- Cache:
  - /cache/ton_core/THONG_TIN_DON
  - /cache/ton_core/HANHTRINH_ALL
  - /cache/TON_AO
- Delta: 
  - /delta/gold_zone/ton_core/THONG_TIN_DON
  - /delta/gold_zone/ton_core/HANHTRINH_ALL

## 3. Output

- HBase:
  - Chi tiết: TON_CHUA_PCP_CHI_TIET
  - Tổng hợp: TON_CHUA_PCP_TONG_HOP
  - Chỉ số trong bảng CHISO_VERSION: ton_chua_pcp
- HDFS: 
  - Chi tiết: /tmp/dungvm/dev/ton_chua_pcp/TON_CHUA_PCP_CHI_TIET
  - Tổng hợp: /tmp/dungvm/dev/ton_chua_pcp/TON_CHUA_PCP_TONG_HOP

  
  **Thay đổi config HDFS_WRITE thành 1 để lưu vào HDFS, 0 để lưu vào HBase**

# Dev

### tạo bảng Hbase
> các bảng này phục vụ chứa dữ liệu đầu ra 
> 
tạo bảng thông qua `$PHOENIX_HOME/bin/pqsl.py` 
```bash
psql.py sql/*.sql
```