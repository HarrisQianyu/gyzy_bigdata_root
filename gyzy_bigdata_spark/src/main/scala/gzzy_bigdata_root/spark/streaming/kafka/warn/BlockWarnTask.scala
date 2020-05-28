package gzzy_bigdata_root.spark.streaming.kafka.warn

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import gzzy_bigdata_root.common.time.TimeTranstationUtils
import gzzy_bigdata_root.redis.client.JedisUtil
import gzzy_bigdata_root.spark.warn.dao.WarningMessageDao
import gzzy_bigdata_root.spark.warn.domain.WarningMessage
import gzzy_bigdata_root.spark.warn.timer.PhoneWarnImpl

/**
  * author: KING
  * description:
  * Date:Created in 2020-03-11 21:57
  */
object BlockWarnTask extends Serializable with Logging{
  /**
    * 黑名单预警   有嫌疑或者已经确定的目标
    * @param DS
    */
  def beginWarn(DS:DStream[java.util.Map[String,String]]): Unit ={

    val array = Array("phone")  //规则字段  需要检查的字段

    DS.foreachRDD(rdd=>{
      if(rdd!=null){
        rdd.foreachPartition(partion=>{
            //获取redis 连接
           val jedis = JedisUtil.getJedis(15)
           //循环进行检查
           while (partion.hasNext){
             val map = partion.next()
              //针对所有字段进行检查
             array.foreach(field=>{
                  //进行比对检查
                 if(map.containsKey(field)){
                   //获取手机号
                   val fieldValue = map.get(field)
                   //只需要判断key相等
                   val redisKey =  field + ":" + fieldValue  //数据中的key
                   val boolean = jedis.exists(redisKey)     //和redis中的规则key进行等值判断
                   if(boolean){
                        //TODO 说明被拦截了
                     //通过时间控制。如果这个消息命中了。那么30秒之内不在预警
                     // 第一次命中了，我是不是耀记录上一次命中的时间 时间存在哪里(redis)
                     // 上一次的命中时间存在redis的规则中  使用warn_time记录上一次命中时间
                     val warn_time = jedis.hget(redisKey,"warn_time")
                     //TODO 假如warn_time不等于空，说明之前命中过，判断时间间隔是不是大于30秒
                      if(StringUtils.isNoneBlank(warn_time)){
                         //说明命中过，进行时间间隔比较
                         //上一次命中时间
                          val warn_time_long = java.lang.Long.valueOf(warn_time)
                         // 当前命中时间
                          val now_time_long = System.currentTimeMillis()/1000
                          if(now_time_long - warn_time_long > 30){
                                //如果间隔大于30秒，再次预警
                            warn(redisKey,jedis,map)
                            //设置本次命中时间
                            jedis.hset(redisKey,"warn_time",System.currentTimeMillis()/1000+"")
                          }
                      }else{
                         //时间等于空，说明是第一次命中, 命中之后需要把本次命中的时间记录到redis
                        warn(redisKey,jedis,map)
                        //设置本次命中时间
                        jedis.hset(redisKey,"warn_time",System.currentTimeMillis()/1000+"")
                      }
                   }else{
                     println("==========数据没有被拦截")
                   }
                 }
             })
           }
        })
      }
    })
  }


  def warn(redisKey:String,jedis:Jedis,map:java.util.Map[String,String]): Unit ={
       //封装告警消息  消息实体类   使用规则信息封装消息体
       //对key进行验证
    val split = redisKey.split(":")
    if(split.length == 2){
      //构造告警消息  WarningMessage
      val warningMessage = new WarningMessage();
      val redisMap = jedis.hgetAll(redisKey)
      warningMessage.setAlarmRuleid(redisMap.get("id"))
      warningMessage.setSendMobile(redisMap.get("send_mobile"))
      warningMessage.setAccountid(redisMap.get("publisher"))
      warningMessage.setSendType(redisMap.get("send_type"))
      warningMessage.setAlarmType("2")

      //构造告警内容
      val warn_type = "【黑名单告警】=》"
      //时间
      val collect_time = map.get("collect_time")
      //地方  经纬度
      val longitude = map.get("longitude")
      val latitude = map.get("latitude")
      //人物
      val phone =  map.get("phone")


      //经纬度和地区转换
      val device_number =  map.get("device_number")
      // 数据库中有设备表
      // 设备ID  设备经纬度  设备所在位置  部署时间  是否正常运行
      // 1111 =》 XXX银行

      val realDate = TimeTranstationUtils.Date2yyyyMMdd_HHmmss(java.lang.Long.valueOf(collect_time+"000"))
      val warn_content = s"${warn_type}【手机号为${phone}】的嫌犯在${realDate}出现在经纬度" +
        s"为【${longitude},${latitude}】附近,具体地址为XXX银行附近 "
      warningMessage.setSenfInfo(warn_content)

      // 存入mysql  构造DAO
      WarningMessageDao.insertWarningMessage(warningMessage)
      // 发出告警消息(微信，QQ，手机)
      //发送规则
      if(warningMessage.getSendType.equals("2")){
          //手机号短信告警，发送短信,   //模拟短信接口
          val warnI = new PhoneWarnImpl()
          warnI.warn(warningMessage)
      }
    }
  }
}
