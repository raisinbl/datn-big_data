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
  SELECT chi_tiet.*
  FROM chi_tiet, chiso_version
  WHERE chi_tiet.version = (
    SELECT MAX(version) FROM chiso_version
  )
"""
query_tongHop = """
  SELECT tong_hop.*
  FROM tong_hop, chiso_version
  WHERE tong_hop.version = (
    SELECT MAX(version) FROM chiso_version
  )
"""
# Streamlit App
st.title("Tổng Hợp Tồn")
# Search bar
search_query = st.text_input("Tìm kiếm", "")
# Execute the query and create a DataFrame
tongHop = pd.read_sql(query_tongHop, conn)
# Filter data based on search query
if search_query:
    filtered_df = tongHop[tongHop.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)]
else:
    filtered_df = tongHop
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