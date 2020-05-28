package gzzy_bigdata_root.spark.common

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-07 20:14
  */
object SparkContextFactory extends Serializable with Logging{

  /**
    * 本地模式
    * @param appName
    * @param threads
    * @return
    */
    def newLocalSparkContext(appName:String="default",threads:Int=2): SparkContext ={
      val sparkConf = SparkConfFactory.newLocalSparkConf(appName,threads)
      return new SparkContext(sparkConf)
    }


  /**
    * 集群模式
    */
  def newYarnSparkContext(): Unit ={
      val sparkConf = SparkConfFactory.newYarnSparkConf()
      return new SparkContext(sparkConf)
    }

}
