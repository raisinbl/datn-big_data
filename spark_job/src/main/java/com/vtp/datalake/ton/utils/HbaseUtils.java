package com.vtp.datalake.ton.utils;

import com.vtp.datalake.ton.config.PhoenixConnectionFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class HbaseUtils extends AbstractDao{
    private static final Logger LOGGER = LogManager.getLogger(HbaseUtils.class);



    public List<Long> getListOldVersions(String ngayBaoCao) {
        List<Long> versionsList = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String dateFilter = String.format("NGAY_BAOCAO < date '%s'", ngayBaoCao);
        String sql = String.format("SELECT NGAY_BAOCAO, VERSION FROM CHISO_VERSION where MA_CHISO = 'log_ton_bcg' and  %s", dateFilter);
        LOGGER.warn(sql);
        try {
            conn = PhoenixConnectionFactory.getInstance().getPhoenixConnection();
            stmt = conn.prepareStatement(sql);
            LOGGER.warn("SQL: " + stmt.toString());
            rs = stmt.executeQuery();
            while (rs.next()) {
                LOGGER.warn(rs.getString("NGAY_BAOCAO"));
                versionsList.add(rs.getLong("VERSION"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            releaseConnect(conn, stmt, rs);
        }
        return versionsList;
    }

    public Long getMaxOldVersion() {

        Long maxVersion = 0l;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String sql = String.format("SELECT MAX(NGAY_BAOCAO) AS NGAY_BAOCAO, MAX(VERSION) AS VERSION FROM CHISO_VERSION where MA_CHISO = 'ton_chua_pcp'");
        LOGGER.warn(sql);
        try {
            conn = PhoenixConnectionFactory.getInstance().getPhoenixConnection();
            stmt = conn.prepareStatement(sql);
            LOGGER.warn("SQL: " + stmt.toString());
            rs = stmt.executeQuery();
            while (rs.next()) {
                LOGGER.warn(rs.getString("NGAY_BAOCAO"));
                maxVersion = rs.getLong("VERSION");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            releaseConnect(conn, stmt, rs);
        }
        return maxVersion;
    }

    public void deleteVersion_tonghop(String tableName, Long version, String ngay_baocao) {

        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        String sql = String.format("DELETE from %s WHERE VERSION = %d AND NGAY_BAOCAO = date '%s'" ,
                tableName, version,ngay_baocao);
        try {
            conn = PhoenixConnectionFactory.getInstance().getPhoenixConnection();
            conn.setAutoCommit(true);
            preparedStatement = conn.prepareStatement(sql);
            LOGGER.warn("SQL: " + preparedStatement.toString());
            preparedStatement.executeUpdate();
            LOGGER.warn("Finish delete");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            releaseConnect(conn, preparedStatement, rs);
        }

    }

    public void deleteVersion_chitiet(String tableName, Long version) {

        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        String sql = String.format("DELETE from %s WHERE VERSION = %d " ,
                tableName, version);
        try {
            conn = PhoenixConnectionFactory.getInstance().getPhoenixConnection();
            conn.setAutoCommit(true);
            preparedStatement = conn.prepareStatement(sql);
            LOGGER.warn("SQL: " + preparedStatement.toString());
            preparedStatement.executeUpdate();
            LOGGER.warn("Finish delete");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            releaseConnect(conn, preparedStatement, rs);
        }

    }

}
