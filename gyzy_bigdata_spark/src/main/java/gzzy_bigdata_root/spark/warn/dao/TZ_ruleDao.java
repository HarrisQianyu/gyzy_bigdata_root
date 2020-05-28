package gzzy_bigdata_root.spark.warn.dao;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.log4j.Logger;
import gzzy_bigdata_root.common.db.DBCommon;
import gzzy_bigdata_root.spark.warn.domain.TZ_RuleDomain;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-11 21:27
 */
public class TZ_ruleDao {
    private static final Logger LOG = Logger.getLogger(TZ_ruleDao.class);


    public static List<TZ_RuleDomain> getRuleList(){
        List<TZ_RuleDomain> listRules = null;
        //数据库连接
        Connection conn = DBCommon.getConn("test");
        QueryRunner query = new QueryRunner();
        String sql = "select * from tz_rule";
        try {
            listRules = query.query(conn,sql,new BeanListHandler<>(TZ_RuleDomain.class));
        } catch (SQLException e) {
            LOG.error(null,e);
            e.printStackTrace();
        }
        return listRules;
    }


    public static void main(String[] args) {
        List<TZ_RuleDomain> ruleList = TZ_ruleDao.getRuleList();
        ruleList.forEach(x->{
            System.out.println(x.toString());
        });
    }

}
