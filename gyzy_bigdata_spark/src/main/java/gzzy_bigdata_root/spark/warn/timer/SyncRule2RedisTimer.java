package gzzy_bigdata_root.spark.warn.timer;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import gzzy_bigdata_root.redis.client.JedisUtil;
import gzzy_bigdata_root.spark.warn.dao.TZ_ruleDao;
import gzzy_bigdata_root.spark.warn.domain.TZ_RuleDomain;

import java.util.List;
import java.util.TimerTask;

/**
 * @author: KING
 * @description: 定时器  同步规则到redis
 * @Date:Created in 2020-03-11 21:24
 */
public class SyncRule2RedisTimer extends TimerTask {
    private static final Logger LOG = Logger.getLogger(SyncRule2RedisTimer.class);

    @Override
    public void run() {
        System.out.println("定时任务开始执行");
        //TODO 把mysql中的规则同步到redis
        //1.获取所有的规则，规则查询
        List<TZ_RuleDomain> ruleList = TZ_ruleDao.getRuleList();
        //遍历所有规则，写入REDIS
        Jedis jedis =null;

        try {
            for (int i = 0; i < ruleList.size(); i++) {
                jedis = JedisUtil.getJedis(15);
                TZ_RuleDomain tz_ruleDomain = ruleList.get(i);
                //获取对象参数
                String id = tz_ruleDomain.getId()+""; //规则ID
                String publisher = tz_ruleDomain.getPublisher(); //发布者
                String warn_fieldname = tz_ruleDomain.getWarn_fieldname();
                String warn_fieldvalue = tz_ruleDomain.getWarn_fieldvalue();
                String send_mobile = tz_ruleDomain.getSend_mobile(); //主要用于告警消息发送
                String send_type = tz_ruleDomain.getSend_type();
                //存入REDIS KEY必须是唯一的  HASH
                String redisKey = warn_fieldname + ":" + warn_fieldvalue; //方便比对
                jedis.hset(redisKey,"id",StringUtils.isNotBlank(id)?id:"");
                jedis.hset(redisKey,"publisher",StringUtils.isNotBlank(publisher)?publisher:"");
                jedis.hset(redisKey,"warn_fieldname",StringUtils.isNotBlank(warn_fieldname)?warn_fieldname:"");
                jedis.hset(redisKey,"warn_fieldvalue",StringUtils.isNotBlank(warn_fieldvalue)?warn_fieldvalue:"");
                jedis.hset(redisKey,"send_mobile",StringUtils.isNotBlank(send_mobile)?send_mobile:"");
                jedis.hset(redisKey,"send_type",StringUtils.isNotBlank(send_type)?send_type:"");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JedisUtil.close(jedis);
        }
        System.out.println("=====================同步规则成功===============" + ruleList.size());
    }
}
