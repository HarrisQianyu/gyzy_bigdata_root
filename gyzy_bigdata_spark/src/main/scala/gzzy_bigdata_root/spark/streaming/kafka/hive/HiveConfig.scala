package gzzy_bigdata_root.spark.streaming.kafka.hive

import org.apache.commons.configuration.{CompositeConfiguration, PropertiesConfiguration}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * author: KING
  * description:
  * Date:Created in 2020-03-14 21:11
  */
object HiveConfig extends Serializable with Logging {

  //配置文件
  val hiveFilePath = "es/mapping/fieldmapping.properties"
  val rootPath = "/user/hive/external" //HDFS根目录

  var config: CompositeConfiguration = null
  var tables: java.util.List[_] = null //所有的表名
  var hiveTableSQL: java.util.HashMap[String, String] = null //存放表名 与 SQL 之间的映射
  var hivePartionTableSQL: java.util.HashMap[String, String] = null //存放表名 与 SQL 之间的映射
  var mapSchema:java.util.HashMap[String, StructType] = null     //存放每种数据类型的StructType


  init()

  def main(args: Array[String]): Unit = {

  }


  def init(): Unit = {

    println("=========================加载配置文件config============================")
    config = readCompositeConfiguration(hiveFilePath)
    val keys = config.getKeys
    while (keys.hasNext) {
      println(keys.next())
    }

    println("=========================初始化tables============================")
    tables = config.getList("tables")
    tables.foreach(table => {
      println(table)
    })


    println("=========================初始化hiveTableSQL============================")
    hiveTableSQL = getHiveSQL
    hiveTableSQL.foreach(table => {
      println(table)
    })


    println("=========================初始化分区SQL============================")
    hivePartionTableSQL = getPartionHiveSQL
    hivePartionTableSQL.foreach(table => {
      println(table)
    })


    println("=========================初始化hiveTableSQL============================")
    mapSchema = getSchema
    mapSchema.foreach(x=>{println(x)})


  }


  /**
    * 获取数据结构
    * @return
    */
  def getSchema(): java.util.HashMap[String, StructType] ={

    val mapStructType = new java.util.HashMap[String, StructType]
    //对表进行遍历构造
    for(table<-tables){
      val arrayStructFields = ArrayBuffer[StructField]()

      //构造一个arrayStructFields
      //获取该类型的所有字段
      val tableFields = config.getKeys(table.toString)
      while (tableFields.hasNext){
        val key = tableFields.next()   //带前缀
        val field = key.toString.split("\\.")(1)  //真实字段
        val fieldType = config.getProperty(key.toString)   //配置文件里面的数据类型

        //模式匹配进行类型转化
        fieldType match {
          case "string" =>arrayStructFields += StructField(field,StringType,true)
          case "long" =>arrayStructFields += StructField(field,StringType,true)
          case "double" =>arrayStructFields += StructField(field,StringType,true)
          case _ =>
        }
      }
      val schema = StructType(arrayStructFields) //接受一个DtructField的Seq
      mapStructType.put(table.toString,schema)
    }
    mapStructType
  }



  /**
    * 构建HIVE  SQL
    *
    * @return
    */
  def getPartionHiveSQL(): java.util.HashMap[String, String] = {
    val hiveSqlMap = new java.util.HashMap[String, String]()
    //使用拼接的方式拼接SQL,对所有的表进行遍历，一个一个的创建
    tables.foreach(table => {
      //开始拼劲
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("

      //拼接字段类型
      val fields = config.getKeys(table.toString)
      //对所有字段进行遍历拼接
      fields.foreach(tableField => {
        //获取真实字段，去掉前缀
        var field = tableField.toString.split("\\.")(1)
        if("table".equals(field)){
          field = "table1"
        }

        //获取真实数据类型
        val fieldType = config.getProperty(tableField.toString)
        //拼接
        sql = sql + field
        //模式匹配 进行类型转换  ES的类型和HIVE的类型不是一一对应的
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })

      sql = sql.substring(0,sql.length -1)
      sql = sql + s") partitioned by (year string,month string,day string) STORED AS PARQUET LOCATION '${rootPath}/${table}'"
      hiveSqlMap.put(table.toString, sql)
    })
    hiveSqlMap
  }




  /**
    * 构建HIVE  SQL
    *
    * @return
    */
  def getHiveSQL(): java.util.HashMap[String, String] = {
    val hiveSqlMap = new java.util.HashMap[String, String]

    tables.foreach(table => {
      //构建sql建表语句，拼接
      var sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //获取这个表类型的所有字段
      val fields = config.getKeys(table.toString)

      //拼接字段
      fields.foreach(tableField => {
        //获取真实字段，去掉前缀
        val field = tableField.toString.split("\\.")(1)
        //获取字段类型
        val fieldType = config.getProperty(tableField.toString)
        //拼接字段
        sql = sql + field
        //拼接类型
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })

      //字段循环完成之后，切掉最后多的一个","
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s") STORED AS PARQUET LOCATION '${rootPath}/qq'"
      hiveSqlMap.put(table.toString, sql)
    })
    hiveSqlMap
  }

  /**
    * 配置文件读取
    *
    * @param path
    * @return
    */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {

    //配置文件的集合，可以包含多个配置文件
    val compositeConfiguration = new CompositeConfiguration()
    val configuration = new PropertiesConfiguration(path)
    compositeConfiguration.addConfiguration(configuration)
    compositeConfiguration
  }


}
