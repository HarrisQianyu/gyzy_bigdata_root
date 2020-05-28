package gzzy_bigdata_root.spark.common

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-14 20:30
  */
object SessionFactory extends Serializable with Logging {


  def newLocalHiveSession(appName: String, threads: Int): SparkSession = {


    System.load("H:\\hadoop-common-2.6.0-bin-master\\bin\\hadoop.dll")
    System.load("H:\\hadoop-common-2.6.0-bin-master\\bin\\winutils.exe")

    val sparkBuild = SparkSession
      .builder()
      .appName(appName)
      .master(s"local[${threads}]")
      .enableHiveSupport()

    val configuration = new Configuration()
    configuration.addResource("spark/hive/core-site.xml")
    configuration.addResource("spark/hive/hdfs-site.xml")
    configuration.addResource("spark/hive/hive-site.xml")

    val iterator = configuration.iterator()
    //遍历，将集群的配置全部设置到sparksession中
    while (iterator.hasNext){
      val next = iterator.next()
      sparkBuild.config(next.getKey,next.getValue)
    }
    sparkBuild.config("spark.driver.allowMultipleContexts","true")
    sparkBuild.getOrCreate()
  }


  /**
    * 本地session  支持hive  不能连接集群，集群的参数没有设置
    *
    * @param appName
    * @param threads
    */
  def newLocalSession(appName: String, threads: Int): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(s"local[${threads}]")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
}
