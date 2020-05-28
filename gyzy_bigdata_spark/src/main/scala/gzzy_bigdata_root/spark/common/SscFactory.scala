package gzzy_bigdata_root.spark.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-07 20:17
  */
object SscFactory extends Serializable with Logging{

  /**
    * 本地模式
    * @param appName
    * @param batchInterval
    * @param threads
    * @return
    */
    def newLocalSSC(appName:String="default",batchInterval:Long=5L,threads:Int=2): StreamingContext ={
      val sparkConf = SparkConfFactory.newLocalSparkConf(appName,threads)
      //控制spark从kafka获取数据的速度
      sparkConf.set("spark.streaming.receiver.maxRate","1000")//每批spark最多获取 1000条数据
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition","10")//kafka topic每个分区每秒最多十条
      new StreamingContext(sparkConf,Seconds(batchInterval))
    }


  /**
    * 本地模式
    * @param batchInterval
    * @return
    */
  def newLocalSSC1(sparkContext:SparkContext,batchInterval:Long=5L): StreamingContext ={
    //控制spark从kafka获取数据的速度
    new StreamingContext(sparkContext,Seconds(batchInterval))
  }


  /**
    * 集群模式
    * @param batchInterval
    * @return
    */
  def newYarnSSC(batchInterval:Long=5L): StreamingContext ={
    val sparkConf = SparkConfFactory.newYarnSparkConf()
    //控制spark从kafka获取数据的速度
    sparkConf.set("spark.streaming.receiver.maxRate","1000")//每批spark最多获取 1000条数据
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","10")//kafka topic每个分区每秒最多十条
    new StreamingContext(sparkConf,Seconds(batchInterval))
  }

}
