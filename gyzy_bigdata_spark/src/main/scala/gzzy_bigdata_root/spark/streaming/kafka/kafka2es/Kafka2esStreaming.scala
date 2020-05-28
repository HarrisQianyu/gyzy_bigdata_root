package gzzy_bigdata_root.spark.streaming.kafka.kafka2es

import org.apache.spark.internal.Logging
import org.elasticsearch.spark.rdd.EsSpark
import gzzy_bigdata_root.common.time.TimeTranstationUtils
import gzzy_bigdata_root.es.admin.AdminUtil
import gzzy_bigdata_root.es.client.ESclientUtil
import gzzy_bigdata_root.spark.common.SscFactory
import gzzy_bigdata_root.spark.common.convert.DataConvert
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig
import gzzy_bigdata_root.spark.streaming.kafka.Spark_Es_ConfigUtil
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper


/**
  * author: KING
  * description:
  * Date:Created in 2020-03-04 22:16
  */
object Kafka2esStreaming extends Serializable with Logging {

  def main(args: Array[String]): Unit = {
    val topic = "test8"
    val groupId = "Kafka2esStreaming"

    val ssc = SscFactory.newLocalSSC("Kafka2esStreaming", 5L, 2)
    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)

    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic)
                        .map(map=>{
                          //添加日期字段，为了按日期分组
                          val collect_time = map.get("collect_time")  //采集时间  时间戳格式
                          // YYYY-MM-DD  引入时间转换工具类
                          val date = TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(collect_time+"000"))
                          map.put("dayPartion",date)

                          //构造地理信息结构
                          val longitude = map.get("longitude")
                          val latitude = map.get("latitude")
                          map.put("location",longitude+","+latitude)

                          map
                        })

    // 写入到ES  按类型,按天 写入  按类型分表   查轨迹， geo地理位置查询
    // 现在的数据全部融合在kafka,需要分组写入
    val array = Array("mail", "wechat")
    array.foreach(table => {
      //TODO 1.按类型分组
      val tableDS = DS.filter(map => {table.equals(map.get("table"))})

      tableDS.foreachRDD(rdd => {
        val client = ESclientUtil.getClient
        //TODO 2.进一步按天分组  按天分组需要一个日期字段  groupby("")
        //取到一个RDD中的所有日期集合
        val arrayDays = rdd.map(x=>(x.get("dayPartion"))).distinct().collect() //把所有数据汇集到driver端
        arrayDays.foreach(day=>{
            //TODO 3.进一步按日期过滤
            val table_day_RDD = rdd.filter(map=>{day.equals(map.get("dayPartion"))})
                                   .map(map=>{
                                     //数据类型转换
                                     DataConvert.strMap2esObjectMap(map)
                                   })
            val index = s"${table}_${day}"
            //自动取创建mapping
            //先判断索引是不是存在
            if(!AdminUtil.indexExists(client,index)){
              //创建新的索引，同时设置mapping
              //按日期进行分索引，一种数据类型每天天会产生一个索引。
              //现在是按数据类型进行分组的，所以这里我们要根据数据类型去获取mapping文件
              //WeChat =》 wechatjson   QQ=>qq_JSON  需要根据类型动态获取mapping文件
              val path = s"es/mapping/${table}.json"
              AdminUtil.buildIndexAndMapping(index,index,path,3,0)
            }

            EsSpark.saveToEs(table_day_RDD,s"${index}/${index}",Spark_Es_ConfigUtil.getEsParam("id"))
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
