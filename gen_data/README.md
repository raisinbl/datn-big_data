
## Dev
Khởi tạo database postgres với định nghĩa các bảng phiếu gửi
```bash
docker compose up -d
```

## Gen Data
- 2 mode gen data dựa trên ngày sinh ra phiếu gửi
  1. current
  2. recent

- 2 hàm: 1 đẩy vào postgres, 2 đẩy thẳng lên hdfs