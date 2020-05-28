package gzzy_bigdata_root.common.db;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gzzy_bigdata_root.common.config.ConfigUtil;

import java.sql.*;
import java.util.Properties;

/**
 * @version : 1.0
 * @description
 * @Date 8:32 2017/8/9
 * @auth :
 */
public class DBCommon {


    private static Logger LOG = LoggerFactory.getLogger(DBCommon.class);
    private static String MYSQL_PATH = "common/mysql.properties";

    //读取配置文件
    private static Properties properties = ConfigUtil.getInstance().getProperties(MYSQL_PATH);

    private static Connection conn ;
    private DBCommon(){}


    public static void main(String[] args) {
        System.out.println(properties);
        Connection tz_bigdata = DBCommon.getConn("test");
        System.out.println(tz_bigdata);
    }

    //TODO  配置文件
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String USER_NAME = properties.getProperty("user");
    private static final String PASSWORD = properties.getProperty("password");
    private static final String IP = properties.getProperty("db_ip");
    private static final String PORT = properties.getProperty("db_port");
    private static final String DB_CONFIG = "?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false";


    //静态代码块，加载驱动
    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            LOG.error(null, e);
        }
    }


    /**
     * 获取数据库连接
     * @param dbName
     * @return
     */
    public static Connection getConn(String dbName) {
        Connection conn = null;
        String  connstring = "jdbc:mysql://"+IP+":"+PORT+"/"+dbName+DB_CONFIG;
        try {
            conn = DriverManager.getConnection(connstring, USER_NAME, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(null, e);
        }
        return conn;
    }



    /**
     * @param url eg:"jdbc:oracle:thin:@172.16.1.111:1521:d406"
     * @param driver eg:"oracle.jdbc.driver.OracleDriver"
     * @param user eg:"ucase"
     * @param password eg:"ucase123"
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */

    public static Connection getConn(String url, String driver, String user,
                                     String password) throws ClassNotFoundException, SQLException{
        Class.forName(driver);
        conn = DriverManager.getConnection(url, user, password);
        return  conn;
    }

    public static void close(Connection conn){
        try {
            if( conn != null ){
                conn.close();
            }
        } catch (SQLException e) {
            LOG.error(null,e);
        }
    }

    public static void close(Statement statement){
        try {
            if( statement != null ){
                statement.close();
            }
        } catch (SQLException e) {
            LOG.error(null,e);
        }
    }

    public static void close(Connection conn,PreparedStatement statement){
        try {
            if( conn != null ){
                conn.close();
            }
            if( statement != null ){
                statement.close();
            }
        } catch (SQLException e) {
            LOG.error(null,e);
        }
    }

    public static void close(Connection conn,Statement statement,ResultSet resultSet) throws SQLException{

        if( resultSet != null ){
            resultSet.close();
        }
        if( statement != null ){
            statement.close();
        }
        if( conn != null ){
            conn.close();
        }
    }

}
