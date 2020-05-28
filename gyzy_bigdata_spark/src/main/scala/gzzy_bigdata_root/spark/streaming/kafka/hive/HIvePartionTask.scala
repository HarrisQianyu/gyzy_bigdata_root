package gzzy_bigdata_root.spark.streaming.kafka.hive

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import gzzy_bigdata_root.common.time.TimeTranstationUtils
import gzzy_bigdata_root.spark.common.{SessionFactory, SscFactory}
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:  缺陷。
  * Date:Created in 2020-03-14 20:44
  */
object HIvePartionTask extends Serializable with Logging{


  def main(args: Array[String]): Unit = {
    //TODO 1.连接HIVE，初始化HIVE表。 动态生成HIVE  SQL
    val spark = SessionFactory.newLocalHiveSession("HIveTest",2)
    spark.sql("use default")
    HiveConfig.hivePartionTableSQL.foreach(map=>{spark.sql(map._2)})


    //TODO 2. 将kafka的数据写入到HDFS   写入parquent格式数据 DF
    val topic = "test8"
    val groupId = "HIveTest"
    val sparkContext = spark.sparkContext
    val ssc = SscFactory.newLocalSSC1(sparkContext,5L)


    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)
    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic)
      .map(map=>{
        //添加分区字段
        //添加日期字段，为了按日期分组
        val collect_time = map.get("collect_time")  //采集时间  时间戳格式
        // YYYY-MM-DD  引入时间转换工具类
        val date = TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(collect_time+"000"))
        val year = date.substring(0,4)
        val month = date.substring(4,6)
        val day = date.substring(6,8)
        map.put("dayPartion",date)
        map.put("year",year)
        map.put("month",month)
        map.put("day",day)
        map
    })


    HiveConfig.tables.foreach(table=>{
      //1.按类型分组
      val tableDS = DS.filter(map => {table.equals(map.get("table"))})
      tableDS.foreachRDD(rdd=>{
         //按日期分组
         val arrayDays = rdd.map(x=>(x.get("dayPartion"))).distinct().collect() //把所有数据汇集到driver端
        //RDD => DF  按表去写
        //定义StructType  放到初始化中去做
        val tableSchema =   HiveConfig.mapSchema.get(table.toString)
        val schemaFields = tableSchema.fieldNames

        arrayDays.foreach(date=>{


          val year = date.substring(0,4)
          val month = date.substring(4,6)
          val day = date.substring(6,8)

         val rowRDD =  rdd.filter(map=>{
            date.equals(map.get("dayPartion"))
          }).map(map=>{
            //把MAP转为row
            val list = new util.ArrayList[Object]()
            for(schemaField <- schemaFields){
              list.add(map.get(schemaField))
            }
            Row.fromSeq(list)
          })

          val tableDF = spark.createDataFrame(rowRDD,tableSchema)
          tableDF.show(2)
          //构建完成DF，开始往HDFS写入
          //定义HDFS目录  分区表目录创建其实就是份目录     /year/month/day
          val tableHdfspATH = s"hdfs://cdh01:8020${HiveConfig.rootPath}/${table}/${year}/${month}/${day}"
          tableDF.write.mode(SaveMode.Append).parquet(tableHdfspATH)

          //TODO 触发脚本，发送一个消息，可以使用消息中间件
          //TODO 可以向kafka 发送触发脚本消息。
          //  date table
          //TODO 3.建立HDFS 与 hive 之间的映射关系
          //设置映射关系
          val sql = s"ALTER TABLE ${table} ADD IF NOT EXISTS PARTITION(year='${year}',month='${month}',day='${day}') LOCATION '${tableHdfspATH}'"
          println("sql=============" + sql)
          spark.sql(sql)
        })

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
