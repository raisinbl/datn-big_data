import streamlit as st
import pandas as pd
from io import BytesIO
import jpype
import jpype.imports
import jpype.dbapi2
import datetime


def init_jvm():
  # Start the JVM
  classpaths = ["/opt/phoenix/phoenix-client-embedded-hbase-2.5.jar", "/opt/phoenix/lib/*"]
  for classpath in classpaths:
    jpype.addClassPath(classpath)
  try:
    jpype.startJVM()
  except Exception as e:
    pass

def export_to_excel(dataframe):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="Sheet1")
    return output.getvalue()

# --------------------------- MAIN ---------------------------
# Start the JVM
init_jvm()
# Connect to the Phoenix database
conn = jpype.dbapi2.connect(dsn="jdbc:phoenix:localhost", driver="org.apache.phoenix.jdbc.PhoenixDriver")
# SQL query
query_chiTiet = """
  SELECT CHI_TIET.*
  FROM CHI_TIET, CHISO_VERSION
  WHERE CHI_TIET.VERSION = (
    SELECT MAX(VERSION) FROM CHISO_VERSION
  )
"""
query_tongHop = """
  SELECT TONG_HOP.*
  FROM TONG_HOP, CHISO_VERSION
  WHERE TONG_HOP.VERSION = (
    SELECT MAX(VERSION) FROM CHISO_VERSION
  )
"""
query_updatedAt = """
  SELECT UPDATED_AT
  FROM CHISO_VERSION
    , (SELECT MAX(VERSION) AS VERSION FROM CHISO_VERSION) AS MAX_VERSION
  WHERE CHISO_VERSION.VERSION = MAX_VERSION.VERSION
"""
updated_at_df = pd.read_sql(query_updatedAt, conn)
updated_at = updated_at_df['UPDATED_AT'][0]
updated_at = datetime.datetime.fromtimestamp(updated_at/1000).strftime("%Y-%m-%d %H:%M:%S")

# Streamlit App
st.title("Tổng Hợp Tồn")
# Get the update time
st.write(f"Updated at: {updated_at}")
# Execute the query and create a DataFrame
tongHop = pd.read_sql(query_tongHop, conn)
# tạo bộ lọc chi nhánh và bưu cục
col1, col2 = st.columns(2)
with col1:
  branch_filter = st.selectbox("Chọn chi nhánh", tongHop['CHI_NHANH_HT'].unique()
                               , index=None, placeholder="Chọn chi nhánh")
  if branch_filter:
    filtered_df = tongHop[tongHop['CHI_NHANH_HT'] == branch_filter]
  else:
    filtered_df = tongHop
with col2:
  if branch_filter:
    post_office_filter = st.selectbox("Chọn bưu cục", tongHop[tongHop['CHI_NHANH_HT'] == branch_filter]['MA_BUUCUC_HT'].unique()
                                      , index=None, placeholder="Chọn bưu cục")
    if post_office_filter:
      filtered_df = filtered_df[filtered_df['MA_BUUCUC_HT'] == post_office_filter]

# Display the table
st.dataframe(filtered_df)
# Export to Excel button

if st.button("Xuất Excel chi tiết"):
    detail_df = pd.read_sql(query_chiTiet, conn)
    excel_data = export_to_excel(detail_df)
    export_date = datetime.datetime.now().strftime("%Y%m%d%H%M")
    st.download_button(
        label="Tải file Excel",
        data=excel_data,
        file_name=f"chi_tiet_ton_{export_date}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )