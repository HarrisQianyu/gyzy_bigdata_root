package gzzy_bigdata_root.spark.streaming.kafka.warn

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream

/**
  * @author: KING
  * @Date:Created in 2020-03-13 22:10
  */
object FlowWarnTask extends Serializable with Logging{


  def beginWarn(DS:DStream[java.util.Map[String,String]]): Unit ={

     //流量預警   spark 10秒統計一次   如果10秒產生的流量超過1M，黃色預警  5M 產生紅色預警
    val DS_new = DS.map(map=>{
        val collect_time = map.get("collect_time")
        java.lang.Long.valueOf(collect_time)
    })

    DS_new.foreachRDD(rdd=>{
      if(rdd != null){
        val flow = rdd.reduce(_+_) //一個批次產生的總流量
        if(flow > 50000){
          println("=====流量大於50000,紅色預警")
        }else if(flow > 10000){
          println("=====流量大於10000,黃色預警")
        }else{
          println("=====流量正常")
        }
      }
    })
  }
}
