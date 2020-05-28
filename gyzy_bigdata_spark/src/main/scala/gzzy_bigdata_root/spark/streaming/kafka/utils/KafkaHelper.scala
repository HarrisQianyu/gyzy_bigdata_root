package gzzy_bigdata_root.spark.streaming.kafka.utils

import com.alibaba.fastjson.{JSON, TypeReference}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import gzzy_bigdata_root.spark.streaming.kafka.offset.ManageOffsetRedis
import gzzy_bigdata_root.spark.streaming.kafka.offset.ManageOffsetRedis.saveOffsetToRedis

import scala.collection.JavaConversions._



class KafkaHelper(val kafkaParams:java.util.Map[String,Object],isUpdate:Boolean=true) extends Serializable with Logging{

  def getInputDSwithOffset(ssc:StreamingContext,
                           kafkaParams:java.util.Map[String,String],
                           groupId:String,
                           topic:String):
  InputDStream[ConsumerRecord[String,String]] ={
    val dbIndex = 1
    val offsetMap = ManageOffsetRedis.getOffsetFromRedis(1,groupId,topic,kafkaParams)
    offsetMap.foreach(x=>{s"初始读取到的offset:${x}"})
    //使用redis中的类型进行类型转换
    val offsets = offsetMap.map(offset=>{
      new TopicPartition(topic,offset._1.toInt)->offset._2.toLong
    }).toMap
    val DS = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String,String](offsets.keys.toList,kafkaParams,offsets))
    DS
  }

  def inputDStoMapDS(inputDS:InputDStream[ConsumerRecord[String,String]]): DStream[java.util.Map[String,String]] ={
    //定义一个函数进行转换
    val converter = {
      json:String=>{
        //定义一个MAP进行存放
        var res :java.util.Map[String,String] = null
        try {
          res = JSON.parseObject(json, new TypeReference[java.util.Map[String, String]]() {})
        } catch {
          case e => logError("json转map失败")
        }
        res
      }
    }
    inputDS.map(consumerRecourd=>converter(consumerRecourd.value()))
  }


  def getMapDSwithOffset(ssc:StreamingContext,
                         kafkaParams:java.util.Map[String,String],
                         groupId:String,
                         topic:String):
  DStream[java.util.Map[String,String]] ={
    val dbIndex = 1
    val offsetMap = ManageOffsetRedis.getOffsetFromRedis(dbIndex,groupId,topic,kafkaParams)
    offsetMap.foreach(x=>{s"初始读取到的offset:${x}"})
    //使用redis中的类型进行类型转换
    val offsets = offsetMap.map(offset=>{
      new TopicPartition(topic,offset._1.toInt)->offset._2.toLong
    }).toMap
    val DS:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String,String](offsets.keys.toList,kafkaParams,offsets))

    //TODO 移到刚获取InputDStream的时候更新
    DS.foreachRDD(rdd=>{
      val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(x=>{
        println(s"获取kafka中的偏移信息${x}")
      })
      if(isUpdate){
        saveOffsetToRedis(dbIndex,groupId,offsetRanges)
      }
    })

    //定义一个函数进行转换
    val converter = {
      json:String=>{
        //定义一个MAP进行存放
        var res :java.util.Map[String,String] = null
        try {
          res = JSON.parseObject(json, new TypeReference[java.util.Map[String, String]]() {})
        } catch {
          case e => logError("json转map失败")
        }
        res
      }
    }
    DS.map(consumerRecourd=>converter(consumerRecourd.value())).filter(x=>x!=null)
  }

}
