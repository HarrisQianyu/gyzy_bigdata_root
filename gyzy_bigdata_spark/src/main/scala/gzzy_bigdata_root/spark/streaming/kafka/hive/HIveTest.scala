package gzzy_bigdata_root.spark.streaming.kafka.hive

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import gzzy_bigdata_root.spark.common.{SessionFactory, SscFactory}
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:  缺陷。
  * Date:Created in 2020-03-14 20:44
  */
object HIveTest extends Serializable with Logging{


  def main(args: Array[String]): Unit = {
    //TODO 1.连接HIVE，初始化HIVE表。 动态生成HIVE  SQL
    val spark = SessionFactory.newLocalHiveSession("HIveTest",2)
    spark.sql("use default")
    HiveConfig.hiveTableSQL.foreach(map=>{spark.sql(map._2)})
    val sparkConf = spark.sparkContext.getConf
    //TODO 2. 将kafka的数据写入到HDFS   写入parquent格式数据 DF
    val topic = "test8"
    val groupId = "HIveTest"
    //val ssc = SscFactory.newLocalSSC1(sparkConf, 5L)

    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))

    val kafkaParams = KafkaConfig.getKafkaConfig(groupId)
    val kafkaHelper = new KafkaHelper(kafkaParams, false)
    val DS = kafkaHelper.getMapDSwithOffset(ssc, kafkaParams.asInstanceOf[java.util.Map[String, String]], groupId, topic)

    HiveConfig.tables.foreach(table=>{
      //1.按类型分组
      val tableDS = DS.filter(map => {table.equals(map.get("table"))})
      tableDS.foreachRDD(rdd=>{
        //RDD => DF  按表去写
         //定义StructType  放到初始化中去做
         val tableSchema =   HiveConfig.mapSchema.get(table.toString)
        val schemaFields = tableSchema.fieldNames
        val rowRDD = rdd.map(map=>{
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
        //定义HDFS目录
        val tableHdfspATH = s"hdfs://cdh01:8020${HiveConfig.rootPath}/${table}"
        tableDF.write.mode(SaveMode.Append).parquet(tableHdfspATH)
        //TODO 3.建立HDFS 与 hive 之间的映射关系
        //设置映射关系
        val sql = s"ALTER TABLE ${table} SET LOCATION '${tableHdfspATH}'"
        spark.sql(sql)
        println(sql)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
