package gzzy_bigdata_root.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-07 20:10
  */
object SparkConfFactory extends Serializable with Logging{

  /**
    * 本地模式SparkConf
    * @param appName
    * @param threads
    */
  def newLocalSparkConf(appName:String="default",threads:Int=2): SparkConf ={
    new SparkConf().setAppName(appName).setMaster(s"local[${threads}]")
  }


  /**
    * 集群模式SparkConf
    */
  def newYarnSparkConf(): SparkConf ={
    new SparkConf()
  }

}
