# Visualize

Phần này sẽ là phần hiển thị data khi dữ liệu đã lưu dưới HBase

Bao gồm các chức năng:

  1. Hiển thị bảng tổng hợp
  2. Tìm kiếm, sort trên bảng tổng hợp
  3. Xuất Excel

## prequisted

1. cài các dependency ở `requirements.txt`
2. đảm bảo các library của phoenix đặt đúng đường dẫn(đặc biệt lưu ý `phoenix-client-embedded-hbase`, và các log lib `slf4j`, `log4j`)

## Run

```python
streamlit run main.py
```
