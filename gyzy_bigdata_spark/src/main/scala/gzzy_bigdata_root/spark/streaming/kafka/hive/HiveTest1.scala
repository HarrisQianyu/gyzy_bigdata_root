package gzzy_bigdata_root.spark.streaming.kafka.hive

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import gzzy_bigdata_root.spark.common.{SessionFactory, SscFactory}
import gzzy_bigdata_root.spark.hive.HiveConf
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig1
import gzzy_bigdata_root.spark.streaming.kafka.utils.KafkaHelper

import scala.collection.JavaConversions._

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-01-15 21:46
  */
object HiveTest1 extends Serializable with Logging{


  def main(args: Array[String]): Unit = {

    val spark = SessionFactory.newLocalHiveSession("SessionFactory",4)
    spark.sql("use default")
    //2.创建HIVE表，读取配置文件，遍历构建
    HiveConfig.hiveTableSQL.foreach(map=>{
      spark.sql(map._2)
    })
    spark.close()

    //TODO 构建DS流
    val kafkaParams = KafkaConfig1.getKafkaConfig("HiveTest")
    val ssc = SscFactory.newLocalSSC("HiveTest",4L,2)
    //val ssc = SscFactory.newSSC(4L)
    val kafkaHelper = new KafkaHelper(kafkaParams.asInstanceOf[java.util.Map[String, Object]],false)
    //createDirectStream  这个是我们自己定义的方法
    val DS = kafkaHelper.getMapDSwithOffset(ssc,kafkaParams,"HiveTest","test8")
    val sparkContext = ssc.sparkContext
    //TODO 动态创建HIVE表
    //1.构造hiveContext
    val context = HiveConf.getHiveContext(sparkContext)
/*    context.sql("use default")

    //2.创建HIVE表，读取配置文件，遍历构建
    HiveConfig.hiveTableSQL.foreach(map=>{
      context.sql(map._2)
      println("============创建表成功")
    })*/


    //TODO 往HDFS写入数据
    HiveConfig.tables.foreach(table=>{
      //按表名（类型）进行过滤数据
     val tableDS =  DS.filter(map=>{table.equals(map.get("table"))})
     //向HDFS写入  使用DF写入  RDD转DF
      tableDS.foreachRDD(rdd=>{

        val tableSchema = HiveConfig.mapSchema.get(table.toString)
        val schemaFields = tableSchema.fieldNames

        //构建rowRDD
        val rowRDD = rdd.map(map=>{
          //需要把map转为row
            val listRow = new util.ArrayList[Object]()
            //填充listRow
            for(schemaField <- schemaFields){
              listRow.add(map.get(schemaField))
            }
            Row.fromSeq(listRow)
        })

        //构建DF
        val tableDF = context.createDataFrame(rowRDD,tableSchema)
        tableDF.show(2)

        //写入HDFS
        //1.定义HDFS目录
        val tableHdfsPath = s"hdfs://had-11:8020${HiveConfig.rootPath}/${table}"
        //2.写入
        tableDF.write.mode(SaveMode.Append).parquet(tableHdfsPath)
        //TODO 建立映射关系

        val sql = s"ALTER TABLE ${table} SET LOCATION '${tableHdfsPath}'"
        println("======================="  + sql)
        context.sql(sql)

      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
