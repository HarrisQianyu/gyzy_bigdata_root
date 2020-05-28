package gzzy_bigdata_root.spark.warn.dao;

import org.apache.log4j.Logger;
import gzzy_bigdata_root.common.db.DBCommon;
import gzzy_bigdata_root.spark.warn.domain.WarningMessage;

import java.sql.*;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-13 20:19
 */
public class WarningMessageDao {
    private static final Logger LOG = Logger.getLogger(WarningMessageDao.class);

    public static Integer insertWarningMessage(WarningMessage warningMessage){

            //获取数据库连接
            Connection conn = DBCommon.getConn("test");
            String sql = "insert into warn_message(alarmRuleid,sendtype,senfinfo,hittime," +
                    "sendmobile,alarmtype) values(?,?,?,?,?,?)";

        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        int id=-1;

        try {
            stmt = conn.prepareStatement(sql);
            stmt.setString(1,warningMessage.getAlarmRuleid());
            stmt.setString(2,warningMessage.getSendType());
            stmt.setString(3,warningMessage.getSenfInfo());
            stmt.setTimestamp(4,new Timestamp(System.currentTimeMillis()));
            stmt.setString(5,warningMessage.getAlarmRuleid());
            stmt.setString(6,warningMessage.getAlarmType());
            id = stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(null,e);
        }finally {
            try {
                DBCommon.close(conn,stmt,resultSet);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return id;
    }

}
